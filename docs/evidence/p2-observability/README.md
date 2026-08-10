# P2 Observability — Evidence Package

## Architecture

```
Trino → Freshness Exporter (Python) → Prometheus (scrape 15s) → Grafana (dashboard)
  ↓                                        ↑
  Iceberg tables:                    Port 9095
  dim_customer_current               Port 3000 (Grafana)
  dim_account_current                Port 9119 (Exporter)
```

## Verification Summary

| Check | Result |
|-------|--------|
| Freshness Exporter responding | ✅ /metrics returns 6 metrics |
| Prometheus scraping exporter | ✅ Target health: "up" |
| Prometheus storing data | ✅ Query returns cdc_freshness_seconds |
| Grafana dashboard loaded | ✅ "Banking Data Platform — CDC Pipeline" |
| Grafana datasource connected | ✅ Prometheus |
| Real metrics visible | ✅ Customer 10K rows, Account 30K rows |
| Freshness detection | ✅ Shows 1.29h/1.58h (red = stale) |

## Metrics Exposed

| Metric | Value | Meaning |
|--------|-------|---------|
| `cdc_freshness_seconds{table="dim_customer_current"}` | ~4648s | 77 min since last consolidation |
| `cdc_freshness_seconds{table="dim_account_current"}` | ~5690s | 95 min since last consolidation |
| `cdc_row_count{table="dim_customer_current"}` | 10000 | Customer records |
| `cdc_row_count{table="dim_account_current"}` | 30000 | Account records |
| `cdc_recent_events` | 0 | No new CDC events in last hour |
| `exporter_up` | 1 | Trino reachable |

## Dashboard Panels (8 panels)

### Pipeline Health (Row 1)
1. **Trino** — UP/DOWN stat (green = up)
2. **Customer Rows** — 10K count stat
3. **Account Rows** — 30K count stat
4. **Recent CDC Events (1h)** — 0 events stat

### Data Freshness (Row 2)
5. **CDC Freshness — Seconds Since Last Consolidation** — Timeseries graph (Customer + Account)
6. **Customer Freshness (Now)** — Current value stat (red if > 1h)
7. **Account Freshness (Now)** — Current value stat (red if > 1h)
8. **Table Row Count Over Time** — Timeseries graph

## Failure Detection Demo

The dashboard correctly shows stale freshness in red:
- **Customer: 1.29 hours** (red background, threshold: 3600s = 1h)
- **Account: 1.58 hours** (red background, threshold: 3600s = 1h)

This means consolidation hasn't run in ~1.5 hours. In production, this would trigger investigation.

After running consolidation, the freshness resets to near-zero, and the color changes to green.

## Key Implementation Details

- **Trino REST API quirk**: Data only present in RUNNING-state responses, not FINISHED
- **Trino auth**: Requires `X-Trino-User: admin` header for all queries
- **Trino SQL**: Use `CAST(... AS BIGINT)` not `BIGINT()`, `TO_UNIXTIME()` not `UNIXTIME()`
- **Caching**: Exporter caches results for 30s to avoid hammering Trino

## Credentials

- Grafana: admin / admin (port 3000)
- Prometheus: port 9095
- Freshness Exporter: port 9119
