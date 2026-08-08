# Banking Data Platform — Architecture

## System Architecture

```mermaid
graph TB
    subgraph Source["Source System"]
        PG[(PostgreSQL 15)]
    end

    subgraph Ingestion["Dual-Path Ingestion"]
        BatchPath["Batch JDBC"]
        CDCPath["WAL CDC"]
    end

    subgraph StreamProcessing["Stream Processing"]
        Debezium["Debezium 2.6"]
        Kafka["Kafka 7.6<br/>(12 topics)"]
        SparkSS["Spark Structured<br/>Streaming"]
    end

    subgraph Lakehouse["Medallion Lakehouse"]
        subgraph Bronze["Bronze Layer"]
            BronzeBatch["16 tables<br/>(source-aligned)"]
            BronzeCDC["6 tables<br/>(append-only events)"]
        end

        subgraph Silver["Silver Layer"]
            SCD2["2 SCD Type 2<br/>dim_customer, dim_account"]
            SCD1["6 SCD Type 1<br/>dim_branch, dim_product, etc."]
            Facts["5 fact tables<br/>fact_txn_account, etc."]
        end

        subgraph Gold["Gold Layer"]
            Marts["18 analytics tables"]
        end
    end

    subgraph Serving["Serving Layer"]
        Trino["Trino 443"]
        dbt["dbt 1.12<br/>(12 models)"]
        Superset["Superset 1.40<br/>(10 dashboards)"]
    end

    subgraph CrossCutting["Cross-Cutting Concerns"]
        Airflow["Airflow 2.10<br/>(17 DAGs)"]
        Contracts["33 Data Contracts"]
        DQ["8 DQ Checks"]
        OpenMeta["OpenMetadata 1.5.6<br/>(53 tables, 22 lineage)"]
        Security["RBAC / Masking / Audit"]
        CICD["GitHub Actions CI/CD"]
    end

    subgraph Storage["Storage"]
        MinIO["MinIO<br/>(S3-compatible)"]
        Iceberg["Iceberg 1.6"]
    end

    PG --> BatchPath
    PG --> CDCPath

    BatchPath --> BronzeBatch
    CDCPath --> Debezium
    Debezium --> Kafka
    Kafka --> SparkSS
    SparkSS --> BronzeCDC

    BronzeBatch --> Silver
    Silver --> Gold

    Gold --> Trino
    Gold --> dbt
    dbt --> Superset

    BronzeBatch -.-> Storage
    BronzeCDC -.-> Storage
    Silver -.-> Storage
    Gold -.-> Storage

    Airflow -.-> BatchPath
    Airflow -.-> SparkSS
    Contracts -.-> BronzeBatch
    DQ -.-> Silver
    OpenMeta -.-> BronzeBatch
    OpenMeta -.-> Silver
    OpenMeta -.-> Gold

    classDef source fill:#e1f5fe,stroke:#0288d1
    classDef stream fill:#fff3e0,stroke:#f57c00
    classDef lake fill:#e8f5e9,stroke:#388e3c
    classDef serve fill:#f3e5f5,stroke:#7b1fa2
    classDef cross fill:#fce4ec,stroke:#c2185b

    class PG source
    class Debezium,Kafka,SparkSS stream
    class BronzeBatch,BronzeCDC,SCD2,SCD1,Facts,Marts lake
    class Trino,dbt,Superset serve
    class Airflow,Contracts,DQ,OpenMeta,Security,CICD cross
```

## Data Flow Summary

### Batch Path (End-to-End)

```
PostgreSQL → Spark JDBC → Bronze Batch (16) → Silver (13) → Gold (18) → Trino/dbt/Superset
```

### CDC Path (Current)

```
PostgreSQL → Debezium → Kafka → Spark Streaming → Bronze CDC (6)
```

> **Note**: CDC consolidation from Bronze CDC to Silver current-state tables is a planned enhancement (P1 in roadmap).

## Key Numbers

| Layer | Count | Description |
|-------|-------|-------------|
| Bronze Batch | 16 | Source-aligned raw tables |
| Bronze CDC | 6 | Append-only change events |
| Silver Dimensions | 8 | 2 SCD Type 2 + 6 SCD Type 1 |
| Silver Facts | 5 | Enriched with dimension keys |
| Gold Marts | 18 | Analytics-ready business products |
| dbt Models | 12 | Semantic layer with tests |
| Airflow DAGs | 17 | Orchestration workflows |
| Data Contracts | 33 | Schema validation |
| DQ Checks | 8 | Quality monitoring |
| OpenMetadata Tables | 53 | Cataloged with lineage |

## Technology Stack

| Component | Version | Role |
|-----------|---------|------|
| PostgreSQL | 15 | Source OLTP database |
| Apache Spark | 3.5.3 | Batch + streaming compute |
| Apache Iceberg | 1.6 | ACID table format |
| MinIO | latest | S3-compatible object storage |
| Iceberg REST Catalog | 1.6 | Shared catalog (Spark + Trino) |
| Trino | 443 | Interactive SQL query engine |
| Apache Airflow | 2.10.0 | Workflow orchestration |
| Debezium | 2.6 | Change Data Capture |
| Apache Kafka | 7.6 | Event streaming |
| OpenMetadata | 1.5.6 | Catalog, lineage, governance |
| dbt Core | 1.12.0 | Semantic models + tests |
| Apache Superset | 1.40.0 | Analytics dashboards |
| Docker Compose | — | Local deployment |

## Port Mapping

| Service | Host Port | Internal Port |
|---------|-----------|---------------|
| PostgreSQL | 5432 | 5432 |
| Spark Master | 9090 | 8080 |
| Spark Worker | 9091 | 8081 |
| Trino | 8085 | 8085 |
| Kafka UI | 8081 | 8080 |
| Airflow | 8080 | 8080 |
| OpenMetadata | 8585 | 8585 |
| Superset | 8088 | 8088 |
| MinIO Console | 9001 | 9001 |

## Related

- [[development-roadmap]] — Next steps: P0 Documentation, P1 CDC Consolidation
- [[cdc-pipeline]] — CDC implementation details
- [[banking-platform]] — Full technology configuration
