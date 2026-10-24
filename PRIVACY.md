# Local pipeline privacy

The pipeline intentionally stores event payloads and repair proposals in its configured database. Model-assisted repair/review transmits selected events to Anthropic. The included signup examples are fictional; operators configure retention for their own event data. Remote API access requires `ETL_ADMIN_TOKEN`; local loopback access is for a trusted single operator. The health check contains no event data. This repository's example emails use the reserved example.com domain.
