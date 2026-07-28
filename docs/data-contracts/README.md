# docs/data-contracts/

Human-readable **data contract catalog** — what each contract means, who owns it,
versioning policy, and breaking-change examples.

## docs/data-contracts/ vs ingestion/schemas/

| Location | Purpose | Audience |
|---|---|---|
| `ingestion/schemas/` | Machine-readable JSON Schema files producers and Flink validate against at runtime | Producers, Flink jobs, CI linting |
| `docs/data-contracts/` | Human-readable contract documentation and version history | Data stewards, downstream consumers |

The JSON files in `docs/data-contracts/` are **frozen snapshots** of the canonical
schemas in `ingestion/schemas/`, kept here for human review and contract negotiation.
When a contract changes, update the canonical schema in `ingestion/schemas/` first,
bump `schema_version` (major for breaking, minor for optional additions), then
copy the new version here with a changelog entry.
