# OpenMetadata Registration — Banking Data Platform

## Overview

This directory contains scripts to register all data assets in OpenMetadata catalog for the Banking Data Platform.

## What's Registered

### Tables (53 total)
- **Bronze Layer**: 21 tables (15 batch + 6 CDC)
- **Silver Layer**: 13 tables (8 dimensions + 5 facts)
- **Gold Layer**: 19 tables (9 history + 9 current)

### Lineage Relationships (22 edges)
- Bronze → Silver: 13 relationships
- Silver → Gold: 9 relationships (dim_customer → all gold tables)

### Tags Applied
- **Tier.Tier1**: 4 critical tables (core_banking_customer, core_banking_account, dim_customer, dim_account)
- **Tier.Tier2**: 13 important tables (transactions, cards, gold tables)
- **Tier.Tier3**: 5 supporting tables (branch, employee, product)
- **PII.Sensitive**: 14 tables containing customer PII data

### Glossary
- **Banking_Glossary**: 8 terms (KYC, PCI_DSS, AML, SCD_Type1, SCD_Type2, RFM, Churn_Risk, AUM)

## Files

- `register_all_tables.sh` — Main registration script (bash)
- `register_tables.py` — Python version (backup)
- `README.md` — This file

## Usage

### Prerequisites
- OpenMetadata running at http://localhost:8585
- Admin credentials: admin / admin (UI auto-encodes with btoa)
- API credentials: admin@open-metadata.org / YWRtaW4= (base64)

### Run Registration
```bash
cd banking_data_platform
bash openmetadata/register_all_tables.sh
```

### What the Script Does
1. Authenticates with OpenMetadata API
2. Cleans up test table (if exists)
3. Registers all 53 tables with proper column definitions
4. Auto-injects `dataLength: 255` for VARCHAR columns (API requirement)

## API Discovery

### Key Findings
1. **VARCHAR columns require `dataLength`** — OpenMetadata API fails without it
   - Solution: Auto-inject `dataLength: 255` in registration script

2. **Glossary endpoint is `/glossaries`** (not `/glossary`)
   - Create glossary: `POST /api/v1/glossaries`
   - Create terms: `POST /api/v1/glossaryTerms`

3. **Tags use JSON Patch format**
   - Content-Type: `application/json-patch+json`
   - Format: `[{"op":"add","path":"/tags","value":[{"tagFQN":"Tier.Tier1"}]}]`

4. **Lineage uses table IDs**
   - Get table ID: `GET /api/v1/tables/name/<fqn>?fields=id`
   - Create lineage: `PUT /api/v1/lineage` with edge JSON

## OpenMetadata UI

Access the catalog at: http://localhost:8585
Login: `admin` / `admin`

### Features Available
- **Browse**: Explore all 53 tables across Bronze/Silver/Gold layers
- **Lineage**: Visualize data flow from source to analytics
- **Tags**: Filter by Tier (importance) and PII (sensitivity)
- **Glossary**: Banking terminology definitions
- **Search**: Full-text search across all metadata

## Data Summary

### Tables by Layer
| Layer | Tables | Key Metrics |
|-------|--------|-------------|
| Bronze | 21 | 30K customers, 90K accounts, 1.8M+ txns |
| Silver | 13 | 10K customers, 30K accounts, 2.4M+ txns |
| Gold | 19 | 20K customers with full analytics |

### Business Analytics
- **RFM Segments**: Champions (22.1%), Potential Loyalists (25.5%), At Risk (15.4%)
- **Churn Risk**: Active (95.6%), High (3.4%)
- **AUM Buckets**: Affluent (50.7%), Priority (36.6%), Mass (12.8%)

## Links

- [[project-complete]] — Full project status
- [[quick-reference]] — Ports and commands
