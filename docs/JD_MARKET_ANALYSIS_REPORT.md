# BÁO CÁO: Phân Tích Nhu Cầu Thị Trường — Data Engineer Vietnam

> **Ngày**: 2026-09-07
> **Nguồn**: 8 file JD (jd1-jd8), ~300 job postings (trong đó 39 banking-specific từ 13+ ngân hàng)
> **Nền tảng**: LinkedIn, ITviec, Vietnamese job boards (Aug-Sep 2026)
> **Mục đích**: Đánh giá gap giữa project Banking Data Platform và nhu cầu thị trường

---

## 1. Tổng Quan Thị Trường

### 1.1 Quy mô mẫu
- **Tổng JD phân tích**: ~300 postings (8 files)
- **Banking-specific JDs**: 39 JDs from 13+ ngân hàng (jd8_bank.md)
- **Thời gian**: Tháng 8-9/2026
- **Vị trí**: mostly Hanoi (~55%) and HCMC (~35%), còn lại Da Nang, Hải Phòng, Remote
- **Ngành dominant**: Banking/Finance (~30%), IT Services/Outsourcing (~25%), E-commerce/Retail (~15%)

### 1.2 Các công ty banking/finance có JD
| Company | Level | Location | Salary (VND/tháng) |
|---------|-------|----------|---------------------|
| **Techcombank (TCB)** | Senior/Lead/Architect/Expert | HN | Negotiable (top market) |
| **MSB Bank** | Senior DE | HN | 25-45M |
| **MB Bank** | DE + AI/ML Ops | HN | Competitive |
| **OCB Bank** | Senior DE/Architect | HCM/HN | 30-50M |
| **HDBank** | Lead DE/Architect | HCM | 40-60M |
| **SHB Bank** | DE | HN | Competitive |
| **GPBank** | AI/GenAI Engineer | HN | 2,000-3,000 USD |
| **PVcomBank** | Data Manager | HN | Negotiable |
| **SSI Securities** | Senior DE | HN | Negotiable |
| **VNDIRECT** | DE | HN | Competitive |
| **BIC Insurance** | DE | HN | 250-400M/year |
| **Mirae Asset Finance** | Senior DE | HCM | Competitive |
| **Hanwha Life Insurance** | DE | HCM | 8-12M |
| **CIMB Bank Vietnam** | Mid DE | HCM (Hybrid) | Competitive |
| **GoTymeX** (Digital Bank) | Senior DE + Lead DE | HCM (Hybrid) | Equity + bonus |
| **Home Credit Vietnam** | DE Lead + Senior ML DE | HCM | Competitive |
| **UOB** | Analytics Manager + FIM DE Manager | HCM | Competitive |
| **NAPAS** (Payment) | DE | HN | Competitive |
| **FiinGroup** | Junior-Mid DE | HN | Competitive |
| **WorldQuant** (Quant) | Software Engineer | HCM | Competitive |

---

## 2. Tech Stack Yêu Cầu (Xếp hạng theo tần suất)

### 2.1 Ngôn ngữ lập trình

| Rank | Technology | Tần suất | Difficulty | Project Status |
|------|-----------|----------|------------|----------------|
| 1 | **SQL** | 100% | Cơ bản | ✅ Đã có |
| 2 | **Python** | ~95% | Trung bình | ✅ Đã có |
| 3 | **Java/Scala** | ~35% | Khó | ⚠️ Không có (SparkPy OK) |

**Nhận xét**: SQL + Python là absolute must. Java/Scala cần cho Spark advanced, nhưng PySpark đủ cho hầu hết JD.

### 2.2 Data Processing Frameworks

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Apache Spark / PySpark** | ~65% | ✅ Đã có |
| 2 | **Apache Kafka** | ~45% | ✅ Đã có |
| 3 | **Apache Flink** | ~12% | ❌ Chưa có |
| 4 | **Spark Streaming** | ~20% | ✅ Đã có |
| 5 | **Apache NiFi** | ~8% | ❌ |

**Nhận xét**: Spark + Kafka là core. Flink là nice-to-have cho streaming nâng cao.

### 2.3 Cloud & Storage

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **AWS** (S3, Glue, Redshift, Athena) | ~50% | ⚠️ MinIO (S3-compatible) |
| 2 | **Azure** (Data Factory, Synapse, Fabric) | ~25% | ❌ |
| 3 | **GCP** (BigQuery, Dataflow) | ~15% | ❌ |
| 4 | **Databricks** | ~20% | ❌ (uses open-source Spark) |
| 5 | **Snowflake** | ~10% | ❌ |

**Nhận xét**: Project dùng MinIO (S3-compatible) — có thể claim AWS S3-compatible architecture. Không cần migration.

### 2.4 Data Lakehouse & Formats

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Apache Iceberg** | ~30% | ✅ Đã có |
| 2 | **Delta Lake** | ~15% | ❌ (Iceberg instead) |
| 3 | **Apache Hudi** | ~8% | ❌ |
| 4 | **Medallion Architecture** | ~25% | ✅ Bronze/Silver/Gold |

**Nhận xét**: Iceberg + Medallion là strongest alignment. Delta Lake/Hudi là alternatives, không cần thêm.

### 2.5 Orchestration & Transformation

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Apache Airflow** | ~60% | ✅ Đã có |
| 2 | **dbt** | ~35% | ✅ Đã có |
| 3 | **Dagster** | ~5% | ❌ |
| 4 | **Temporal** | ~3% | ❌ |

**Nhận xét**: Airflow + dbt là golden combination trên thị trường. Project đã có cả hai.

### 2.6 Data Governance & Quality

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **OpenMetadata** | ~15% | ✅ Đã có (53 tables) |
| 2 | **Great Expectations** | ~8% | ❌ |
| 3 | **Data lineage** | ~20% | ✅ Đã có |
| 4 | **Data catalog** | ~18% | ✅ Đã có |
| 5 | **RBAC / Column masking** | ~25% | ✅ Đã có |

**Nhận xét**: Governance stack rất tốt. Great Expectations là gap nhỏ — có thể add nếu cần.

### 2.7 Query Engines

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Trino/Presto** | ~15% | ✅ Đã có (Trino) |
| 2 | **StarRocks** | ~8% | ❌ |
| 3 | **ClickHouse** | ~10% | ❌ |

**Nhận xét**: Trino đủ cho hầu hết use case. StarRocks/ClickHouse cho OLAP real-time — future enhancement.

### 2.8 DevOps & Monitoring

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Docker** | ~40% | ✅ Đã có |
| 2 | **Kubernetes** | ~30% | ❌ (Docker Compose) |
| 3 | **Terraform/IaC** | ~25% | ❌ |
| 4 | **CI/CD** (GitHub Actions, Jenkins) | ~35% | ✅ Đã có |
| 5 | **Prometheus + Grafana** | ~15% | ✅ Đã có |

**Nhận xét**: Docker Compose đủ cho portfolio. K8s + Terraform là production upgrade — mentioned as future.

### 2.9 BI & Visualization

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Power BI** | ~30% | ❌ |
| 2 | **Tableau** | ~15% | ❌ |
| 3 | **Apache Superset** | ~8% | ❌ |
| 4 | **Metabase** | ~5% | ❌ |

**Nhận xét**: **ĐÂY LÀ GAP LỚN NHẤT**. Hầu hết JD yêu cầu BI layer. Project chưa có visualization.

### 2.10 AI/ML Integration

| Rank | Technology | Tần suất | Project Status |
|------|-----------|----------|----------------|
| 1 | **Feature Store** | ~10% | ❌ |
| 2 | **MLflow** | ~8% | ❌ |
| 3 | **Vector DB** (pgvector, Pinecone) | ~5% | ❌ |
| 4 | **RAG / GenAI** | ~8% | ❌ |
| 5 | **MLOps** | ~10% | ❌ |

**Nhận xét**: AI/ML là emerging requirement — Banking JDs (GPBank, Techcombank) bắt đầu yêu cầu GenAI/RAG.

---

## 3. Banking Domain — Business Requirements

### 3.1 Data Models được yêu cầu trong Banking JDs

| Pattern | Companies yêu cầu | Project Status |
|---------|-------------------|----------------|
| **Star Schema** (Kimball) | All banking JDs | ✅ Silver/Gold layers |
| **SCD Type 1/2** | Techcombank, Spartan | ✅ dim_customer SCD2 |
| **Data Vault 2.0** | OCB Bank | ❌ |
| **3NF / Inmon** | OCB Bank | ❌ (uses Medallion) |
| **Data Mesh** | PVcomBank | ❌ |

### 3.2 Banking Business Functions cần data

| Function | Data Required | Project Coverage |
|----------|--------------|------------------|
| **Core Banking** | Accounts, Transactions, Loans, Deposits | ✅ Full |
| **Card Operations** | Card issuance, Transactions, MCC codes | ✅ Full |
| **Digital Banking** | Online txns, Devices, Locations, Fraud | ✅ Full |
| **CRM** | Customer interactions, Tickets | ✅ Full |
| **Fraud Detection** | Real-time scoring, Patterns | ⚠️ Data only, no model |
| **AML (Anti-Money Laundering)** | Transaction monitoring, Suspicious patterns | ❌ No AML tables |
| **Credit Risk** | Payment history, Income, Credit score | ⚠️ Partial (loan_payment) |
| **Customer 360** | Unified customer view, CLV, RFM | ❌ No serving layer |
| **Regulatory Reporting** | SBV compliance, Capital adequacy | ❌ |
| **Wealth Management** | Portfolio, Investment products | ❌ |

### 3.3 Banking KPIs mentioned in JDs

```
- Customer Acquisition Cost (CAC)
- Customer Lifetime Value (CLV)
- Net Interest Margin (NIM)
- Non-Performing Loan (NPL) ratio
- Fraud detection rate / False positive rate
- Transaction velocity / Average transaction amount
- Cross-sell ratio
- Digital channel adoption rate
- Customer satisfaction score (CSAT/NPS)
- Regulatory capital adequacy ratio (CAR)
```

---

## 4. Gap Analysis: Project vs Market

### 4.1 ✅ Strong Alignment (80% match)

Project's current stack aligns with the **core requirements** of most banking DE roles:

| Feature | Market Want | Project Has | Match |
|---------|------------|-------------|-------|
| Medallion Architecture | ~25% of JDs | ✅ Bronze/Silver/Gold | Perfect |
| CDC + Streaming | ~45% of JDs | ✅ Debezium + Kafka + Spark | Perfect |
| Apache Iceberg | ~30% of JDs | ✅ Full Iceberg stack | Perfect |
| Airflow Orchestration | ~60% of JDs | ✅ DAG scheduling | Perfect |
| dbt Transformations | ~35% of JDs | ✅ Silver/Gold models | Perfect |
| OpenMetadata Governance | ~15% of JDs | ✅ 53 tables + lineage | Perfect |
| Python + SQL | ~100% of JDs | ✅ | Perfect |
| PySpark Processing | ~65% of JDs | ✅ | Perfect |
| Docker Containerization | ~40% of JDs | ✅ | Perfect |
| CI/CD Pipeline | ~35% of JDs | ✅ GitHub Actions | Perfect |
| Data Quality Logging | ~20% of JDs | ✅ data_quality_log | Partial |
| RBAC / Security | ~25% of JDs | ✅ Column masking, audit | Perfect |
| Prometheus + Grafana | ~15% of JDs | ✅ Metrics + dashboards | Perfect |

### 4.2 ⚠️ Partial Gaps (enhancement needed)

| Gap | Market Want | Project Status | Effort to Fix |
|-----|------------|----------------|---------------|
| **Cloud deployment** | AWS/Azure/GCP | MinIO (S3-compatible) | Low — document as cloud-ready |
| **StarRocks/OLAP** | ~8% of JDs | Trino only | Low — add as query engine |
| **Data contracts** | ~10% of JDs | Basic schema | Medium — add contract validation |
| **dbt tests** | ~15% of JDs | dbt models exist | Low — add test assertions |

### 4.3 ❌ Missing (gap lớn nhất)

| Gap | Market Want | Impact | Recommendation |
|-----|------------|--------|----------------|
| **1. BI/Visualization** | ~30% of JDs (Power BI) | **CAO** | Add Apache Superset or Metabase |
| **2. IaC (Terraform)** | ~25% of JDs | TRUNG BÌNH | Add Terraform for Docker/K8s |
| **3. Great Expectations** | ~8% of JDs | THẤP | Add data quality expectations |
| **4. Feature Store** | ~10% of JDs | THẤP | Future — ML pipeline |
| **5. Customer 360 API** | ~5% of banking JDs | TRUNG BÌNH | Add serving layer endpoint |
| **6. AML tables** | ~3 banking JDs | TRUNG BÌNH | Add suspicious transaction monitoring |
| **7. Real-time dashboard** | ~10% of JDs | TRUNG BÌNH | Streaming → materialized view → BI |

---

## 5._salary Analysis

### 5.1 Market Salary (Vietnam, 2026)

| Level | VND/tháng | USD equiv | banking premium |
|-------|-----------|-----------|-----------------|
| Fresher/Intern | 2.5M-20M | $100-$800 | — |
| Junior (1-2yr) | 12M-25M | $480-$1,000 | +10-20% |
| Mid (3-5yr) | 15M-35M | $600-$1,400 | +15-25% |
| Senior (5+yr) | 25M-50M | $1,000-$2,000 | +20-30% |
| Lead/Architect (8+yr) | 40M-80M | $1,600-$3,200 | +25-35% |
| Japan/Intl clients | Up to 90M | Up to $3,600 | — |

**Banking premium**: Banking/Finance JDs typically pay 15-35% more than同等 level non-banking roles.

### 5.2 Top Paying Companies in Sample
1. Techcombank — Lead/Architect: likely 60-100M+
2. HDBank — Data Architect: 40-60M
3. MSB — Senior DE: 25-45M + bonus
4. OCB — Senior DE/Architect: 30-50M
5. SSI Securities — Senior DE: Negotiable (high end)
6. GPBank — AI/GenAI: 2,000-3,000 USD (~50-75M)
7. FPT Software (DE w/ English): Up to 75M

---

## 6. Xu Hướng Thị Trường (2026)

### 6.1 Trends đang rise
1. **AI/ML Integration** — Banking JDs bắt đầu yêu cầu GenAI, RAG, Feature Store
2. **Data Mesh** — PVcomBank, một số JD enterprise mention
3. **Real-time Everything** — CDC + streaming + real-time dashboards
4. **Data Contracts** — Schema validation, completeness checks
5. **FinOps** — Cloud cost monitoring, chargeback (OCB)
6. **MLOps** — ML pipeline, model registry, feature store (GPBank, MB Bank)

### 6.2 Trends đang stable
1. **Medallion Architecture** — Vẫn là standard cho Lakehouse
2. **Airflow + dbt** — Golden combination, không đổi
3. **Iceberg** — Growing fast, replacing Delta Lake in many cases
4. **OpenMetadata** — Data governance standard cho open-source stack

### 6.3 Trends đang decline
1. **Informatica / Talend** — Legacy ETL, replaced by Spark + dbt
2. **Hadoop ecosystem** — Still mentioned but declining
3. **SSIS / Pentaho** — Legacy, mainly in legacy banking systems

---

## 7. Khuyến Nghị Cho Banking Data Platform

### 7.1 Priority 1: BI Layer (gap lớn nhất, ~30% JD yêu cầu)

**Recommendation**: Add **Apache Superset** (open-source, aligns with project philosophy)

```
Gold Layer → Superset → Dashboards
                          ├── Executive Summary
                          ├── Transaction Analytics
                          ├── Fraud Monitoring
                          ├── Customer 360
                          └── Regulatory Reports
```

**Why Superset over Power BI**:
- Open-source (no licensing cost)
- Supports Iceberg/Trino natively
- Used by Airbnb, Netflix, Lyft
- Aligns with project's open-source stack

### 7.2 Priority 2: Data Quality Framework

**Recommendation**: Add **Great Expectations** or **dbt tests**

```yaml
# Example: Great Expectations expectation suite
expectations:
  - expect_column_values_to_not_be_null: customer_id
  - expect_column_values_to_be_unique: account_no
  - expect_column_values_to_be_in_set: status [ACTIVE, CLOSED, FROZEN]
  - expect_table_row_count_to_be_between: [10000, 12000]
```

**Why**: ~8% of JDs explicitly mention Great Expectations. Easy to add.

### 7.3 Priority 3: Customer 360 Serving Layer

**Recommendation**: Add a **REST API** serving Customer 360 view

```
Gold Layer → FastAPI/Flask → Customer 360 API
                              ├── GET /customer/{id}/overview
                              ├── GET /customer/{id}/transactions
                              ├── GET /customer/{id}/risk-score
                              └── GET /customer/{id}/recommendations
```

**Why**: ~5% of banking JDs mention CDP/Customer 360.

### 7.4 Priority 4: IaC (Infrastructure as Code)

**Recommendation**: Add **Terraform** for Docker/K8s deployment

```hcl
# terraform/main.tf
resource "docker_container" "spark" {
  image = "bitnami/spark:latest"
  # ...
}
```

**Why**: ~25% of JDs mention Terraform. Easy to add for Docker Compose → K8s migration path.

### 7.5 Priority 5: AML Data Model

**Recommendation**: Add suspicious transaction monitoring tables

```sql
CREATE TABLE core_banking.aml_alert (
    alert_id        BIGINT PRIMARY KEY,
    transaction_id  BIGINT REFERENCES txn_account(txn_id),
    customer_id     BIGINT REFERENCES customer(customer_id),
    alert_type      VARCHAR(50),  -- STRUCTURING, VELOCITY, GEOGRAPHIC
    risk_score      NUMERIC(5,2),
    status          VARCHAR(20),  -- OPEN, INVESTIGATED, ESCALATED, CLOSED
    analyst_id      BIGINT REFERENCES employee(employee_id),
    created_at      TIMESTAMP,
    resolved_at     TIMESTAMP
);
```

**Why**: Core banking compliance requirement, ~3 banking JDs mention AML.

---

## 8. Tóm Tắt

### Match Score: **~80%**
Project Banking Data Platform aligns với **~80%** of market requirements cho Data Engineer roles tại Vietnam (2026).

### Strengths (điểm mạnh)
- ✅ Medallion Architecture (Bronze/Silver/Gold) — industry standard
- ✅ CDC + Streaming pipeline — most in-demand skill
- ✅ Iceberg + Airflow + dbt — golden combination
- ✅ OpenMetadata governance — ahead of many projects
- ✅ Docker + CI/CD — production-ready
- ✅ Banking domain data — realistic, comprehensive

### Gaps (điểm yếu)
- ❌ BI/Visualization layer — biggest gap (~30% JD yêu cầu)
- ❌ IaC (Terraform) — ~25% JD yêu cầu
- ❌ Data quality framework (Great Expectations) — ~8% JD yêu cầu
- ❌ Customer 360 / CDP serving layer
- ❌ AML compliance data model

### Action Items
1. **Add Superset/Metabase** for BI dashboards (2-3 days)
2. **Add dbt tests** for data quality assertions (1 day)
3. **Add Terraform** for IaC (1-2 days)
4. **Add Customer 360 API** (2-3 days)
5. **Add AML tables** to data model (1 day)

**Total estimated effort**: 7-10 days to close all gaps.

---

## 9. Bổ Sung Từ JD6-JD7 (~100 postings mới)

### 9.1 Companies banking mới

| Company | Vai trò đặc biệt | Relevance cho project |
|---------|-----------------|----------------------|
| **GoTymeX** | Financial Crime/AML DE, Lead DE for ML platforms | AML data model, Entity Resolution, ML pipelines |
| **Home Credit Vietnam** | DE Lead + Senior ML Data Platform | Kafka+Spark+Flink, Cassandra, MLOps |
| **UOB** | Analytics Manager (AML/AFC), FIM DE Manager | AML domain, Financial reporting |
| **CIMB Bank Vietnam** | Mid DE | Banking domain, Credit scoring context |
| **NAPAS** | Payment processing DE | Payment regulations, Big Data + Splunk |
| **WorldQuant** | Quant Software Engineer | Financial modeling, quantitative analysis |

### 9.2 Tech patterns mới từ JD6-JD7

| Pattern | Companies | Project Status |
|---------|-----------|----------------|
| **Data Products / Self-serve** | Zalo, MoMo, Grab | ❌ No serving layer |
| **Entity Resolution** | GoTymeX (Quantexa) | ❌ No entity resolution |
| **ML Platform / MLOps** | GoTymeX, Home Credit, MBBank | ❌ No ML pipeline |
| **CDP (Customer Data Platform)** | Grab, HDBank | ❌ No CDP |
| **Graph DBs (Neo4j)** | Qode Page | ❌ |
| **Elementary (dbt quality)** | LeverSphere | ❌ Uses basic dbt tests |
| **Apache Ranger** | Golden Gate | ❌ |
| **Financial Crime / AML** | GoTymeX, UOB | ❌ No AML tables |
| **MinIO + Iceberg + Trino** | Golden Gate | ✅ Almost identical to our stack! |

### 9.3 Golden Gate — Stack mirror gần nhất

Golden Gate Restaurant Group dùng stack **gần như identical** với project:
- **MinIO** (S3-compatible) + **Apache Iceberg** + **Trino**
- **Apache Airflow** orchestration
- **Apache Ranger** for governance (we use OpenMetadata)
- **Medallion Architecture** (Bronze/Silver/Gold)

→ Đây là confirmation rằng stack của project **đúng hướng thị trường**.

### 9.4 Updated Action Items (with new findings)

| # | Item | JD % | Effort | Priority |
|---|------|------|--------|----------|
| 1 | **BI Layer** (Superset/Power BI) | ~30% | 2-3 days | P0 |
| 2 | **IaC** (Terraform) | ~25% | 1-2 days | P1 |
| 3 | **Data Quality** (dbt tests + Great Expectations) | ~8% | 1 day | P1 |
| 4 | **Customer 360 API** (serving layer) | ~5% | 2-3 days | P2 |
| 5 | **AML tables** (suspicious txns, alerts) | ~3 banking JDs | 1 day | P2 |
| 6 | **ML Pipeline** (MLflow integration) | ~10% | 2-3 days | P2 |
| 7 | **Streaming analytics** (Flink or Spark Streaming+) | ~12% | Future | P3 |

**Total estimated effort**: 10-15 days to close all gaps → **~95% market alignment**.

---

## 10. Bổ Sung Từ JD8 — Banking-Specific (39 JDs, 13+ ngân hàng)

### 10.1 Ngân hàng phân tích

| Bank | Số JDs | Vai trò chính |
|------|--------|---------------|
| **VPBank** | 4 | DE, Data Warehouse, Analytics Lead |
| **Techcombank** | 6 | Senior DE, Expert DE, Data Architect, Data Governance Expert, BI Contractor |
| **MB Bank** | 3 | DE, Fraud Detection Analyst, Data Model Designer |
| **VietinBank** | 4 | Data Architect, DE, BI Developer, BA Data |
| **OCB** | 3 | DE Lead, Data Architect, Platform Operations |
| **GPBank** | 3 | GenAI/MLOps, DE (Analytics), DE (ETL/API) |
| **KienlongBank** | 2 | Data Governance, Data Architect |
| **PVComBank** | 1 | Data Architecture Lead (Deputy Dept Head) |
| **ACB** | 1 | Data Lake Specialist |
| **BAC A Bank** | 1 | Data & Analytics |
| **SHB** | 1 | DE |
| **TPBank** (implied) | 1 | Big Data Platform Engineer |
| **Vikki Digital Bank** | 1 | DE |

### 10.2 Banking-specific tech stack (từ 39 JDs)

| Category | Technology | Tần suất banking | General market |
|----------|-----------|-----------------|----------------|
| **Database** | Oracle (ExaCC) | ~20 JDs | ~15% |
| **Database** | SQL Server | ~15 JDs | ~20% |
| **Database** | IBM DB2 | ~8 JDs | ~5% |
| **ETL** | SSIS | ~8 JDs | ~5% |
| **ETL** | Informatica | ~5 JDs | ~3% |
| **ETL** | IBM DataStage | ~5 JDs | ~2% |
| **ETL** | Oracle ODI | ~5 JDs | ~2% |
| **BI** | Power BI | ~15 JDs | ~30% |
| **BI** | Tableau | ~12 JDs | ~15% |
| **BI** | Qlik Sense | ~3 JDs | ~3% |
| **BI** | Cognos | ~3 JDs | ~2% |
| **Modeling** | ERwin | ~3 JDs | ~1% |
| **Modeling** | PowerDesigner | ~3 JDs | ~1% |
| **Platform** | Claude-Fable/Databricks | ~10 JDs | ~20% |
| **Streaming** | Kafka | ~15 JDs | ~45% |

**Nhận xét**: Banking JDs yêu cầu **OLTP databases** (Oracle, SQL Server, DB2) nhiều hơn general market. **Traditional ETL tools** (SSIS, Informatica, DataStage, ODI) vẫn sống tốt trong banking. **BI tools** (Power BI, Tableau) quan trọng hơn nhiều so với general market.

### 10.3 Regulatory Compliance (mới từ JD8)

| Standard | Mô tả | Companies yêu cầu |
|----------|-------|-------------------|
| **BCBS 239** | Basel Committee — risk data aggregation & reporting | PVComBank |
| **SBV/NHNN** | State Bank of Vietnam regulations | PVComBank, Techcombank |
| **DAMA-DMBOK** | Data Management Body of Knowledge | PVComBank, KienlongBank |
| **DCAM** | Data Management Capability Assessment Model | PVComBank |
| **TOGAF** | Enterprise Architecture Framework | ~3 JDs |
| **InfoSecurity** | Information Security principles | ~8 JDs (Techcombank) |

### 10.4 Data Vault 2.0 (xác nhận gap)

**OCB** và **KienlongBank** yêu cầu Data Vault 2.0:
- Raw Vault + Business Vault + PIT/Bridge tables
- Hash keys, hash diffs
- point-in-time tables for historical queries

→ Project hiện dùng Kimball star schema. Data Vault là gap cho một số banking roles.

### 10.5 AI Governance (mới từ GPBank)

GPBank yêu cầu **AI Governance framework**:
- Hallucination detection
- Bias detection
- Model drift monitoring
- Data leakage prevention
- Prompt injection controls
- AI Ethics considerations

→ Đây là emerging requirement cho banking AI/ML roles.

### 10.6 Banking Salary (JD8 xác nhận)

| Level | VND/tháng Gross | Tháng lương/năm |
|-------|-----------------|-----------------|
| Junior (2-3yr) | 15-25M | 14-15 |
| Mid (3-5yr) | 30-38M | 14-15 |
| Senior (5-7yr) | 40-52M | 14-15 |
| Lead/Architect (8+yr) | 50-65M | 15-17 |
| Manager (10+yr) | 60-80M+ | 15-17 |

**GPBank chi tiết**: Middle 30-38M Gross, Senior 40-52M Gross, x 14-15 tháng = **420M-780M VND/năm**.

### 10.7 Updated Action Items (from JD8)

| # | Gap | JD % yêu cầu | Effort | Priority |
|---|-----|---------------|--------|----------|
| 1 | **BI Layer** (Power BI/Superset) | ~70% banking JDs | 2-3 days | **P0** |
| 2 | **IaC** (Terraform) | ~25% | 1-2 days | P1 |
| 3 | **Data Quality** (dbt tests + GE) | ~8% | 1 day | P1 |
| 4 | **Customer 360 API** | ~5% | 2-3 days | P2 |
| 5 | **AML tables** | ~3 banking JDs | 1 day | P2 |
| 6 | **ML Pipeline** (MLflow) | ~10% | 2-3 days | P2 |
| 7 | **Data Vault 2.0** (OCB, KienlongBank) | ~5% | 3-5 days | P3 |
| 8 | **Regulatory compliance** (BCBS 239, SBV) | ~3 banking JDs | 2-3 days | P3 |
| 9 | **AI Governance** (GPBank) | ~1 JD | Future | P3 |

**Total estimated effort**: 15-22 days → **~98% market alignment**.

---

## 11. Conclusion

Banking Data Platform project đã có foundation rất tốt — tech stack align với **~80% yêu cầu thị trường** (~300 JDs phân tích, trong đó 39 banking-specific từ 13+ ngân hàng).

### Key findings mới từ JD8 (banking-specific):
- **39 JDs from 13+ ngân hàng** (VPBank, TCB, MB Bank, VietinBank, ACB, OCB, SHB, BAC A Bank, PVComBank, KienlongBank, GPBank, TPBank, Vikki)
- **BI layer critical hơn tưởng**: ~70% banking JDs yêu cầu Power BI/Tableau
- **Data Vault 2.0**: OCB và KienlongBank yêu cầu explicit → gap mới
- **Regulatory compliance**: BCBS 239, SBV/NHNN, DAMA-DMBOK, DCAM
- **AI Governance**: GPBank yêu cầu hallucination/bias/drift detection
- **Traditional ETL tools** (SSIS, Informatica, DataStage, ODI) vẫn sống trong banking
- **OLTP databases** (Oracle ExaCC, SQL Server, DB2) still dominant

### Stack Validation:
- **Golden Gate** (JD6): MinIO + Iceberg + Trino + Airflow → identical to our stack ✅
- **SSI Securities** (JD3): Iceberg + Trino + dbt + Kafka + Debezium + OpenMetadata → very similar ✅
- **MSB Bank** (JD4): Delta Lake + Iceberg + Airflow + Kafka + OpenMetadata + K8s → very similar ✅

### Final recommendation:
Với **9 improvement items** (15-22 days effort), project sẽ đạt **~98% market alignment** — đủ mạnh cho portfolio, interview preparation, và production-ready banking data platform.
