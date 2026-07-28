# Cursor Guidance Layout

Cursor reads project-specific guidance from `.cursor/rules/*.mdc`.

This repository keeps guidance in three layers:

1. `.cursor/rules/*.mdc` — Cursor's active, scoped agent rules.
2. `AGENTS.md` — cross-agent and human-readable operating guide.
3. `.cursorrules` — legacy compatibility entry point for tools that still
   look for the old root file.

Do not copy the same long instructions into all three places. Keep the
specific, enforceable Cursor behavior in `.cursor/rules`, keep architecture
and runbook references in `AGENTS.md`, and keep `.cursorrules` short.

## Branches

| Branch | Role |
|---|---|
| `main` | Fully cloud-deployed (AWS). |
| `local-testing-version` | Local Docker / Flink / DuckDB testing. |

## Rule Scope

| Rule | Scope |
|---|---|
| `project-context.mdc` | Always applies |
| `python.mdc` | `**/*.py` |
| `dbt.mdc` | `transformation/dbt_project/**` |
| `flink.mdc` | `streaming/**` |
| `airflow.mdc` | `orchestration/**` |
| `terraform.mdc` | `infra/terraform/**` |

When adding a new rule, keep it focused (~60 lines) and use Cursor `.mdc`
frontmatter:

```markdown
---
description: Short description
globs: "path/**"
alwaysApply: false
---
```
