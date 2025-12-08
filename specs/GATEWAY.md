# Gateway Integration Specification

## Overview

Conduktor Gateway is a Kafka proxy that intercepts Kafka protocol requests and enables:
- **Virtual Topics**: Logical topics mapped to physical Kafka topics with transformations
- **Interceptors**: Plugins that transform, filter, or mask data at read/write time
- **Alias Topics**: Map one topic name to another (useful for migrations, versioning)

streamt integrates with Gateway to enable the `virtual_topic` materialization type.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         streamt CLI                                   │
│                                                                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐ │
│  │   Compiler   │───▶│   Manifest   │───▶│   Gateway Deployer       │ │
│  │              │    │              │    │                          │ │
│  │ virtual_topic│    │ gateway_rules│    │  PUT /gateway/v2/...     │ │
│  └──────────────┘    └──────────────┘    └────────────┬─────────────┘ │
└───────────────────────────────────────────────────────┼───────────────┘
                                                        │
                                                        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     Conduktor Gateway                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
│  │  Alias Topics   │  │  Interceptors   │  │  Virtual Clusters       │ │
│  │                 │  │                 │  │                         │ │
│  │ virtual → real  │  │ filter, mask,   │  │ tenant isolation        │ │
│  └─────────────────┘  │ encrypt, etc.   │  └─────────────────────────┘ │
│                       └─────────────────┘                              │
│  Port 6969 (Proxy)                           Port 8888 (Admin API)    │
└───────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────────┐
│                           Kafka Cluster                                │
│                     (real topics live here)                           │
└───────────────────────────────────────────────────────────────────────┘
```

## Gateway API v2

### Base URL
```
http://localhost:8888/gateway/v2
```

### Authentication
HTTP Basic Auth with admin credentials:
```
Authorization: Basic base64(username:password)
```

Default: `admin:conduktor`

### Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/interceptor` | List all interceptors |
| PUT | `/interceptor` | Create/update interceptor |
| DELETE | `/interceptor/{name}` | Delete interceptor |
| GET | `/alias-topic` | List alias topic mappings |
| PUT | `/alias-topic` | Create/update alias topic |
| DELETE | `/alias-topic` | Delete alias topic (body: `{"name": "..."}`) |

### Interceptor Types

| Type | Use Case |
|------|----------|
| `io.conduktor.gateway.interceptor.safeguard.ClientIdRequiredPolicyPlugin` | Require client.id |
| `io.conduktor.gateway.interceptor.safeguard.ReadOnlyTopicPolicyPlugin` | Prevent writes |
| `io.conduktor.gateway.interceptor.FieldLevelEncryptionPlugin` | Encrypt fields |
| `io.conduktor.gateway.interceptor.MaskingPlugin` | Mask sensitive data |
| `io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin` | SQL-based virtual topics |

## Virtual Topics in streamt

### YAML Definition

```yaml
models:
  - name: payments_pii_masked
    materialized: virtual_topic
    description: Payments with PII masked for analytics consumers
    sql: |
      SELECT * FROM {{ source("payments_raw") }}
      WHERE amount > 0
    security:
      policies:
        - mask:
            column: customer_email
            method: partial
            for_roles: [analytics]
        - mask:
            column: credit_card
            method: hash
```

### Compiled Artifact

The compiler generates a `GatewayRuleArtifact`:

```json
{
  "name": "payments_pii_masked",
  "virtualTopic": "payments_pii_masked",
  "physicalTopic": "payments.raw.v1",
  "interceptors": [
    {
      "type": "filter",
      "config": {
        "where": "amount > 0"
      }
    },
    {
      "type": "mask",
      "config": {
        "field": "customer_email",
        "method": "partial",
        "forRoles": ["analytics"]
      }
    },
    {
      "type": "mask",
      "config": {
        "field": "credit_card",
        "method": "hash"
      }
    }
  ]
}
```

### Deployer Actions

1. **Create alias topic** mapping `payments_pii_masked` → `payments.raw.v1`
2. **Create interceptors** for filtering and masking
3. **Apply to scope** (global, virtual cluster, or specific user group)

## Configuration

### Project Configuration

```yaml
# stream_project.yml
runtime:
  gateway:
    admin_url: http://localhost:8888
    proxy_bootstrap: localhost:6969
    username: admin
    password: ${GATEWAY_PASSWORD}
    virtual_cluster: default  # optional, for multi-tenant setups
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_ADMIN_URL` | `http://localhost:8888` | Gateway Admin API URL |
| `GATEWAY_PROXY_BOOTSTRAP` | `localhost:6969` | Gateway proxy for Kafka clients |
| `GATEWAY_USERNAME` | `admin` | Admin API username |
| `GATEWAY_PASSWORD` | `conduktor` | Admin API password |

## Use Cases

### 1. Read-Time Filtering (Virtual View)

No storage cost - filter applied when consumers read:

```yaml
- name: high_value_orders
  materialized: virtual_topic
  sql: SELECT * FROM {{ source("orders") }} WHERE amount > 10000
```

**When to use**: Low-volume filtered views, ad-hoc analytics, development.

**Trade-off**: Filter runs on every read (CPU at Gateway).

### 2. PII Masking per Role

Different consumers see different data:

```yaml
- name: customers_view
  materialized: virtual_topic
  from:
    - source: customers_raw
  security:
    policies:
      - mask:
          column: ssn
          method: redact
          for_roles: [support]
      - mask:
          column: email
          method: partial
          for_roles: [analytics]
```

### 3. Topic Migration / Aliasing

Map old topic name to new without changing consumers:

```yaml
- name: orders_v1  # consumers still use this name
  materialized: virtual_topic
  from:
    - source: orders_v2  # actual topic
```

### 4. Schema Evolution Protection

Filter out invalid records at read time:

```yaml
- name: events_validated
  materialized: virtual_topic
  sql: |
    SELECT * FROM {{ source("events") }}
    WHERE event_type = 'click'
```

> **Note**: Gateway VirtualSqlTopicPlugin supports limited SQL syntax: `=`, `>`, `<`, `>=`, `<=`, `<>`, `REGEXP`, and `AND`.
> Not supported: `IN`, `OR`, `IS NULL`, `IS NOT NULL`, `LIKE`, `BETWEEN`.

## virtual_topic vs topic

| Aspect | `topic` | `virtual_topic` |
|--------|---------|-----------------|
| Storage | Creates real Kafka topic | No storage (alias) |
| Latency | Pre-computed, fast reads | Filter on read |
| Consistency | Point-in-time snapshot | Always current |
| CPU | At write time (Flink) | At read time (Gateway) |
| Gateway required | No | Yes |
| Masking per role | No | Yes |

**Rule of thumb**:
- High-read, low-filter ratio → `topic` (pre-materialize)
- Low-read, high-filter ratio → `virtual_topic` (compute on demand)
- Need role-based masking → `virtual_topic`

## Deployer Implementation

### GatewayDeployer Class

```python
class GatewayDeployer:
    """Deployer for Conduktor Gateway interceptors and alias topics."""

    def __init__(
        self,
        admin_url: str,
        username: str = "admin",
        password: str = "conduktor",
        virtual_cluster: str | None = None,
    ):
        self.admin_url = admin_url.rstrip("/")
        self.auth = (username, password)
        self.virtual_cluster = virtual_cluster

    def apply(self, artifact: GatewayRuleArtifact) -> str:
        """Deploy a gateway rule. Returns action taken."""
        ...

    def delete(self, name: str) -> bool:
        """Delete a gateway rule by name."""
        ...

    def list_rules(self) -> list[dict]:
        """List all interceptors."""
        ...
```

### API Mapping

| streamt Artifact | Gateway API Call |
|------------------|------------------|
| `GatewayRuleArtifact.virtual_topic` | `PUT /alias-topic` |
| `GatewayRuleArtifact.interceptors[type=filter]` | `PUT /interceptor` (VirtualSqlTopicPlugin) |
| `GatewayRuleArtifact.interceptors[type=mask]` | `PUT /interceptor` (MaskingPlugin) |

## Error Handling

| Scenario | Error Type | User Message |
|----------|------------|--------------|
| Gateway not configured | `ConfigurationError` | "virtual_topic requires Gateway. Configure runtime.gateway or use materialized: topic" |
| Gateway unreachable | `ConnectionError` | "Cannot connect to Gateway at {url}. Is it running?" |
| Invalid interceptor config | `ValidationError` | "Invalid mask method '{method}'. Supported: hash, partial, redact, null" |
| Unauthorized | `AuthenticationError` | "Gateway authentication failed. Check username/password" |

## Testing

### Integration Test Setup

1. Gateway included in docker-compose.yml
2. `InfrastructureConfig` includes `gateway` property
3. Tests verify:
   - Alias topic creation
   - Interceptor deployment
   - Consumer sees transformed data through proxy port

### Test Scenarios

```python
def test_virtual_topic_deployment():
    """Virtual topic creates alias and interceptors."""

def test_filter_interceptor():
    """SQL WHERE clause becomes filter interceptor."""

def test_mask_interceptor():
    """Security policy creates masking interceptor."""

def test_consumer_through_gateway():
    """Consumer via port 6969 sees transformed data."""
```

## Roadmap

### Phase 1 (Complete)
- [x] Gateway in docker-compose
- [x] GatewayDeployer implementation
- [x] Alias topic support
- [x] Basic filter interceptor (VirtualSqlTopicPlugin)
- [x] Integration tests (25 tests)

### Phase 2
- [ ] Masking interceptor support
- [ ] Role-based policies
- [ ] Virtual cluster support

### Phase 3
- [ ] Encryption interceptor
- [ ] Gateway status in `streamt status`
- [ ] Gateway rules in lineage diagram
