# Optional authenticated front door for the dashboard (enable_auth = true).
#
# Without this file's resources the App Runner service is public HTTPS with no
# login. With them:
#
#   internet -> ALB (public, Cognito Hosted UI login) -> VPC interface endpoint
#     -> App Runner service (private, is_publicly_accessible = false)
#
# The ALB targets the endpoint ENIs by IP; ALB does not validate the target's
# TLS certificate, so the App Runner-managed cert behind the endpoint works.
# Extra cost: ALB (~$16/mo + LCU) plus one interface endpoint per AZ (~$7/mo
# each). Cognito stays in free tier for any realistic demo audience.

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

locals {
  auth_prereqs_missing = var.enable_auth && (
    var.auth_domain_name == "" ||
    var.acm_certificate_arn == "" ||
    length(var.alb_subnet_ids) < 2 ||
    length(var.vpce_subnet_ids) == 0
  )
}

# --- Cognito: user pool + Hosted UI client ------------------------------------

resource "aws_cognito_user_pool" "dashboard" {
  count = var.enable_auth ? 1 : 0
  name  = "${local.name_prefix}-dashboard"

  auto_verified_attributes = ["email"]

  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_uppercase = true
    require_numbers   = true
    require_symbols   = true
  }

  # INACTIVE so `terraform destroy` can remove the pool on a demo teardown.
  deletion_protection = "INACTIVE"

  tags = local.tags
}

resource "aws_cognito_user_pool_domain" "dashboard" {
  count        = var.enable_auth ? 1 : 0
  domain       = "${var.project}-${var.environment}-dash-${data.aws_caller_identity.current.account_id}"
  user_pool_id = aws_cognito_user_pool.dashboard[0].id
}

resource "aws_cognito_user_pool_client" "alb" {
  count           = var.enable_auth ? 1 : 0
  name            = "${local.name_prefix}-dashboard-alb"
  user_pool_id    = aws_cognito_user_pool.dashboard[0].id
  generate_secret = true

  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["openid", "email", "profile"]
  supported_identity_providers         = ["COGNITO"]
  explicit_auth_flows                  = ["ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH"]

  # The ALB's Cognito action completes the code flow at this fixed path.
  callback_urls = ["https://${var.auth_domain_name}/oauth2/idpresponse"]
  logout_urls   = ["https://${var.auth_domain_name}/"]
}

# --- Security groups ------------------------------------------------------------

resource "aws_security_group" "alb" {
  count       = var.enable_auth ? 1 : 0
  name_prefix = "${local.name_prefix}-dash-alb-"
  description = "Public ALB for the dashboard: HTTPS from auth_allowed_cidrs."
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTPS (Cognito login + app)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.auth_allowed_cidrs
  }

  ingress {
    description = "HTTP (redirects to HTTPS)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.auth_allowed_cidrs
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-dash-alb-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "vpce" {
  count       = var.enable_auth ? 1 : 0
  name_prefix = "${local.name_prefix}-dash-vpce-"
  description = "App Runner interface endpoint: HTTPS from the dashboard ALB only."
  vpc_id      = var.vpc_id

  ingress {
    description     = "HTTPS from dashboard ALB"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.alb[0].id]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-dash-vpce-sg" })

  lifecycle {
    create_before_destroy = true
  }
}

# Separate rule resource so the two groups don't reference each other inline,
# which Terraform would reject as a dependency cycle.
resource "aws_security_group_rule" "alb_to_vpce" {
  count                    = var.enable_auth ? 1 : 0
  type                     = "egress"
  security_group_id        = aws_security_group.alb[0].id
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.vpce[0].id
  description              = "HTTPS to the App Runner interface endpoint"
}

# --- Private ingress to App Runner ----------------------------------------------

resource "aws_vpc_endpoint" "apprunner" {
  count               = var.enable_auth ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.apprunner.requests"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.vpce_subnet_ids
  security_group_ids  = [aws_security_group.vpce[0].id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-dash-apprunner-vpce" })
}

resource "aws_apprunner_vpc_ingress_connection" "this" {
  count       = var.enable_auth && var.create_service ? 1 : 0
  name        = "${local.name_prefix}-dash-ingress"
  service_arn = aws_apprunner_service.this[0].arn

  ingress_vpc_configuration {
    vpc_id          = var.vpc_id
    vpc_endpoint_id = aws_vpc_endpoint.apprunner[0].id
  }

  tags = local.tags
}

# --- ALB ------------------------------------------------------------------------

resource "aws_lb" "dashboard" {
  count                      = var.enable_auth ? 1 : 0
  name                       = "${local.name_prefix}-dash"
  load_balancer_type         = "application"
  internal                   = false
  subnets                    = var.alb_subnet_ids
  security_groups            = [aws_security_group.alb[0].id]
  drop_invalid_header_fields = true
  # Streamlit holds a websocket open; keep it above the default 60s.
  idle_timeout = 120

  tags = local.tags

  lifecycle {
    precondition {
      condition     = !local.auth_prereqs_missing
      error_message = "enable_auth requires auth_domain_name, acm_certificate_arn, >= 2 alb_subnet_ids, and >= 1 vpce_subnet_ids."
    }
  }
}

data "aws_network_interface" "vpce" {
  for_each = var.enable_auth ? toset(aws_vpc_endpoint.apprunner[0].network_interface_ids) : toset([])
  id       = each.key
}

resource "aws_lb_target_group" "apprunner" {
  count       = var.enable_auth ? 1 : 0
  name        = "${local.name_prefix}-dash"
  vpc_id      = var.vpc_id
  target_type = "ip"
  protocol    = "HTTPS"
  port        = 443

  health_check {
    protocol = "HTTPS"
    path     = "/_stcore/health"
    matcher  = "200"
  }

  tags = local.tags
}

resource "aws_lb_target_group_attachment" "vpce" {
  for_each         = data.aws_network_interface.vpce
  target_group_arn = aws_lb_target_group.apprunner[0].arn
  target_id        = each.value.private_ip
  port             = 443
}

resource "aws_lb_listener" "https" {
  count             = var.enable_auth ? 1 : 0
  load_balancer_arn = aws_lb.dashboard[0].arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.acm_certificate_arn

  default_action {
    type = "authenticate-cognito"
    authenticate_cognito {
      user_pool_arn       = aws_cognito_user_pool.dashboard[0].arn
      user_pool_client_id = aws_cognito_user_pool_client.alb[0].id
      user_pool_domain    = aws_cognito_user_pool_domain.dashboard[0].domain
      # Re-login every 12h instead of the default 7d.
      session_timeout = 43200
    }
  }

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.apprunner[0].arn
  }
}

resource "aws_lb_listener" "http_redirect" {
  count             = var.enable_auth ? 1 : 0
  load_balancer_arn = aws_lb.dashboard[0].arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}
