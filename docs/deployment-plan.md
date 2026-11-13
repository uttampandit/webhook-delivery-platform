# Deployment plan

**Status: NOT REQUESTED.** The completed release target is local Compose on one operator-controlled machine. No cloud account, service, repository publication, or public endpoint has been created.

## If hosting is requested later

First prepare a provider-specific plan and obtain the user's explicit choice before creating accounts or incurring costs. The service needs:

- An always-on Go API and worker; a sleeping worker changes recovery timing.
- Durable PostgreSQL with restricted network access, backups, and a restore test.
- Private secret storage and rotation for API/control tokens and both signing keys.
- TLS and access restrictions for any shared demonstration; PostgreSQL, receiver controls, and metrics stay private.
- Health and restart policy, resource and disk limits, monitoring, retention policy, and an abuse budget.
- A tested deployment and rollback procedure, plus the limits of the receiver-side effect guarantee.

No provider or region is selected, so no price estimate is asserted. A later estimate must name the provider, region, instance/database sizes, backup and egress assumptions, currency, and checked date. Free tiers or suspended workers cannot be assumed to preserve the local recovery model.

## Pending decisions and actions

1. User explicitly requests hosting and chooses a public portfolio, private/VPN demo, or another target.
2. Compare current provider offerings and costs for always-on compute and durable PostgreSQL.
3. Decide private access, domain/TLS, secret management, backups, retention, quotas, and abuse controls.
4. Verify the resulting provider-specific architecture and recovery model before any deployment.

Until those actions are authorized and verified, local readiness must not be described as public deployment or production readiness.
