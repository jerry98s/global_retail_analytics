{{ config(materialized='view') }}

/*
  OneID identity resolution — thin dbt view over the Spark GraphFrames output.

  Since ADR-010, edge construction and connected components run in the Spark
  job `spark/identity_resolution/identity_resolution_job.py`, which writes
  `silver.identity_resolution` (Iceberg). This view only adds the dbt
  surrogate `identity_key`; all business rules (edge types, public-device
  exclusion, rep priority, confidence/method, customer_key formula) live in
  `spark/identity_resolution/graph_logic.py`.

  Consumers are unchanged: sessions, RFM, dim_customer, fact_sales and the
  identity_graph mart all read this model. Public devices remain here at 0.3
  confidence for audit; marketing.identity_graph filters them out.

  Fixture mode (`-DbtSource seeds`, CI): the source resolves to
  seeds/silver/identity_resolution.csv, generated from the bronze fixture
  CSVs by spark/identity_resolution/generate_fixture.py.

  See:
    - docs/data-model/identity-resolution.md
    - docs/decisions/ADR-003-identity-graph.md
    - docs/decisions/ADR-010-spark-graphframes-identity.md
*/

select
    {{ generate_surrogate_key(['identifier_type', 'identifier_value']) }} as identity_key,
    cast(identifier_type as varchar)        as identifier_type,
    cast(identifier_value as varchar)       as identifier_value,
    cast(customer_key as bigint)            as customer_key,
    cast(confidence_score as decimal(5, 4)) as confidence_score,
    cast(resolution_method as varchar)      as resolution_method,
    cast(is_public_device as boolean)       as is_public_device,
    cast(component_rep_node as varchar)     as component_rep_node,
    cast(component_rep_type as varchar)     as component_rep_type
from {{ source('silver', 'identity_resolution') }}
