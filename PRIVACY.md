# Local pipeline privacy

The pipeline intentionally stores event payloads and repair proposals in its configured database. It is not a zero-retention service. Model-assisted repair/review transmits selected events to Anthropic. Use fictional signup examples; do not send production personal data without appropriate authorization and a retention policy. Remote API access requires `ETL_ADMIN_TOKEN`; local loopback access is for a trusted single operator. The health check contains no event data. This repository's example emails use the reserved example.com domain.
