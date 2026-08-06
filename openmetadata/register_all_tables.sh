#!/bin/bash
# =============================================================================
# Register all tables in OpenMetadata for Banking Data Platform
# =============================================================================

# Get JWT token
TOKEN=$(curl -s -X POST http://localhost:8585/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d '{"email": "admin@open-metadata.org", "password": "YWRtaW4="}' | grep -o '"accessToken":"[^"]*"' | cut -d'"' -f4)

echo "Token obtained: ${TOKEN:0:20}..."

# Function to inject dataLength for VARCHAR columns missing it
inject_data_length() {
    echo "$1" | sed 's/\("dataType":"VARCHAR"\)/\1,"dataLength":255/g' | sed 's/"dataLength":255,"dataLength":255/"dataLength":255/g'
}

# Function to register table
register_table() {
    local schema=$1
    local table=$2
    local description=$3
    local columns=$4

    # Auto-inject dataLength for VARCHAR columns
    local fixed_columns
    fixed_columns=$(inject_data_length "$columns")

    RESULT=$(curl -s -X POST http://localhost:8585/api/v1/tables \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"name\": \"$table\",
        \"displayName\": \"$(echo $table | sed 's/_/ /g' | sed 's/\b\(.\)/\u\1/g')\",
        \"description\": \"$description\",
        \"databaseSchema\": \"TrinoLakehouse.lakehouse.$schema\",
        \"columns\": $fixed_columns
      }")

    if echo "$RESULT" | grep -q "\"name\":\"$table\""; then
        echo "  ✅ $schema.$table"
    else
        echo "  ❌ $schema.$table: $(echo $RESULT | grep -o '"message":"[^"]*"' | head -1)"
    fi
}

# Delete test table first
echo "Cleaning up test table..."
curl -s -X DELETE http://localhost:8585/api/v1/tables/TrinoLakehouse.lakehouse.bronze.test_table \
  -H "Authorization: Bearer $TOKEN" > /dev/null 2>&1

echo ""
echo "Registering BRONZE tables..."
echo "============================="

# Bronze - Core Banking
register_table "bronze" "core_banking_customer" "Core banking customer data" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"cccd","dataType":"VARCHAR","dataLength":20},{"name":"full_name","dataType":"VARCHAR","dataLength":200},{"name":"gender","dataType":"VARCHAR","dataLength":10},{"name":"date_of_birth","dataType":"DATE"},{"name":"phone","dataType":"VARCHAR","dataLength":20},{"name":"email","dataType":"VARCHAR","dataLength":200},{"name":"address","dataType":"VARCHAR","dataLength":500},{"name":"city","dataType":"VARCHAR","dataLength":100},{"name":"district","dataType":"VARCHAR","dataLength":100},{"name":"branch_code","dataType":"VARCHAR","dataLength":10},{"name":"customer_segment","dataType":"VARCHAR","dataLength":50},{"name":"kyc_status","dataType":"VARCHAR","dataLength":20},{"name":"register_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_banking_account" "Core banking account data" '[{"name":"account_id","dataType":"BIGINT"},{"name":"account_no","dataType":"VARCHAR","dataLength":20},{"name":"customer_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR","dataLength":10},{"name":"branch_code","dataType":"VARCHAR","dataLength":10},{"name":"account_type","dataType":"VARCHAR","dataLength":20},{"name":"currency","dataType":"VARCHAR","dataLength":3},{"name":"balance","dataType":"DECIMAL"},{"name":"open_date","dataType":"DATE"},{"name":"close_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR","dataLength":20},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_banking_branch" "Core banking branch data" '[{"name":"branch_code","dataType":"VARCHAR","dataLength":10},{"name":"branch_name","dataType":"VARCHAR","dataLength":200},{"name":"city","dataType":"VARCHAR","dataLength":100},{"name":"region","dataType":"VARCHAR","dataLength":50},{"name":"manager_id","dataType":"BIGINT"},{"name":"open_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"}]'

register_table "bronze" "core_banking_employee" "Core banking employee data" '[{"name":"employee_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"position","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"hire_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"}]'

register_table "bronze" "core_banking_loan" "Core banking loan data" '[{"name":"loan_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"loan_type","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"interest_rate","dataType":"DECIMAL"},{"name":"term_months","dataType":"INT"},{"name":"start_date","dataType":"DATE"},{"name":"end_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_banking_txn_account" "Core banking account transactions" '[{"name":"txn_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"txn_date","dataType":"TIMESTAMP"},{"name":"description","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"counter_account","dataType":"VARCHAR"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_banking_product" "Core banking product data" '[{"name":"product_code","dataType":"VARCHAR"},{"name":"product_name","dataType":"VARCHAR"},{"name":"product_type","dataType":"VARCHAR"},{"name":"interest_rate","dataType":"DECIMAL"},{"name":"is_active","dataType":"INT"}]'

register_table "bronze" "core_banking_deposit" "Core banking deposit data" '[{"name":"deposit_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"deposit_type","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"interest_rate","dataType":"DECIMAL"},{"name":"start_date","dataType":"DATE"},{"name":"maturity_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR"}]'

# Bronze - Card CRM
register_table "bronze" "card_crm_card" "Card CRM card data" '[{"name":"card_id","dataType":"BIGINT"},{"name":"card_no_masked","dataType":"VARCHAR"},{"name":"customer_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"card_type","dataType":"VARCHAR"},{"name":"card_brand","dataType":"VARCHAR"},{"name":"credit_limit","dataType":"DECIMAL"},{"name":"issue_date","dataType":"DATE"},{"name":"expiry_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "card_crm_card_txn" "Card CRM card transactions" '[{"name":"txn_id","dataType":"BIGINT"},{"name":"card_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"txn_date","dataType":"TIMESTAMP"},{"name":"txn_amount","dataType":"DECIMAL"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"currency","dataType":"VARCHAR"},{"name":"merchant_name","dataType":"VARCHAR"},{"name":"merchant_category","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"created_ts","dataType":"TIMESTAMP"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "card_crm_crm_interaction" "Card CRM interactions" '[{"name":"interaction_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"interaction_type","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"subject","dataType":"VARCHAR"},{"name":"description","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"created_at","dataType":"TIMESTAMP"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

# Bronze - Digital Banking
register_table "bronze" "digital_banking_device" "Digital banking device data" '[{"name":"device_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"device_type","dataType":"VARCHAR"},{"name":"device_name","dataType":"VARCHAR"},{"name":"os_version","dataType":"VARCHAR"},{"name":"app_version","dataType":"VARCHAR"},{"name":"last_login","dataType":"TIMESTAMP"},{"name":"is_active","dataType":"INT"}]'

register_table "bronze" "digital_banking_location" "Digital banking location data" '[{"name":"location_id","dataType":"BIGINT"},{"name":"merchant_name","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"latitude","dataType":"DECIMAL"},{"name":"longitude","dataType":"DECIMAL"},{"name":"mcc_code","dataType":"VARCHAR"}]'

register_table "bronze" "digital_banking_online_transaction" "Digital banking online transactions" '[{"name":"transaction_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"device_id","dataType":"BIGINT"},{"name":"location_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"transaction_type","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"currency","dataType":"VARCHAR"},{"name":"is_fraud","dataType":"VARCHAR"},{"name":"fraud_reason","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"transaction_date","dataType":"TIMESTAMP"},{"name":"created_ts","dataType":"TIMESTAMP"},{"name":"last_updated","dataType":"TIMESTAMP"}]'

register_table "bronze" "digital_banking_support_ticket" "Digital banking support tickets" '[{"name":"ticket_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"ticket_type","dataType":"VARCHAR"},{"name":"priority","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"subject","dataType":"VARCHAR"},{"name":"description","dataType":"VARCHAR"},{"name":"created_at","dataType":"TIMESTAMP"},{"name":"resolved_at","dataType":"TIMESTAMP"}]'

# Bronze - CDC Tables
register_table "bronze" "core_customer_cdc" "CDC - Core customer real-time updates" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"cccd","dataType":"VARCHAR"},{"name":"full_name","dataType":"VARCHAR"},{"name":"gender","dataType":"VARCHAR"},{"name":"date_of_birth","dataType":"BIGINT"},{"name":"phone","dataType":"VARCHAR"},{"name":"email","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"district","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"kyc_status","dataType":"VARCHAR"},{"name":"register_date","dataType":"BIGINT"},{"name":"is_active","dataType":"VARCHAR"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_account_cdc" "CDC - Core account real-time updates" '[{"name":"account_id","dataType":"BIGINT"},{"name":"account_no","dataType":"VARCHAR"},{"name":"customer_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"account_type","dataType":"VARCHAR"},{"name":"currency","dataType":"VARCHAR"},{"name":"balance","dataType":"DECIMAL"},{"name":"open_date","dataType":"BIGINT"},{"name":"close_date","dataType":"BIGINT"},{"name":"status","dataType":"VARCHAR"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

register_table "bronze" "core_transaction_cdc" "CDC - Core transaction real-time updates" '[{"name":"txn_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"txn_date","dataType":"BIGINT"},{"name":"txn_amount","dataType":"DECIMAL"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"debit_credit","dataType":"VARCHAR"},{"name":"balance_after","dataType":"DECIMAL"},{"name":"channel","dataType":"VARCHAR"},{"name":"description","dataType":"VARCHAR"},{"name":"counter_account","dataType":"VARCHAR"},{"name":"created_ts","dataType":"BIGINT"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

register_table "bronze" "card_account_cdc" "CDC - Card account real-time updates" '[{"name":"card_id","dataType":"BIGINT"},{"name":"card_no_masked","dataType":"VARCHAR"},{"name":"customer_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"card_type","dataType":"VARCHAR"},{"name":"card_brand","dataType":"VARCHAR"},{"name":"credit_limit","dataType":"DECIMAL"},{"name":"issue_date","dataType":"BIGINT"},{"name":"expiry_date","dataType":"BIGINT"},{"name":"status","dataType":"VARCHAR"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

register_table "bronze" "card_transaction_cdc" "CDC - Card transaction real-time updates" '[{"name":"txn_id","dataType":"BIGINT"},{"name":"card_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"txn_date","dataType":"BIGINT"},{"name":"txn_amount","dataType":"DECIMAL"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"currency","dataType":"VARCHAR"},{"name":"merchant_name","dataType":"VARCHAR"},{"name":"merchant_category","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"created_ts","dataType":"BIGINT"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

register_table "bronze" "online_transaction_cdc" "CDC - Online transaction real-time updates" '[{"name":"transaction_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"device_id","dataType":"BIGINT"},{"name":"location_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"transaction_type","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"currency","dataType":"VARCHAR"},{"name":"is_fraud","dataType":"VARCHAR"},{"name":"fraud_reason","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"transaction_date","dataType":"BIGINT"},{"name":"created_ts","dataType":"BIGINT"},{"name":"last_updated","dataType":"BIGINT"},{"name":"__cdc_operation","dataType":"VARCHAR"},{"name":"__cdc_timestamp","dataType":"TIMESTAMP"},{"name":"__cdc_timestamp_ms","dataType":"BIGINT"},{"name":"__spark_batch_id","dataType":"BIGINT"},{"name":"__ingestion_time","dataType":"TIMESTAMP"}]'

echo ""
echo "Registering SILVER tables..."
echo "============================"

# Silver - Dimensions
register_table "silver" "dim_branch" "SCD Type 1 - Branch dimension" '[{"name":"branch_sk","dataType":"BIGINT"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"branch_name","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"region","dataType":"VARCHAR"},{"name":"manager_id","dataType":"BIGINT"},{"name":"open_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_product" "SCD Type 1 - Product dimension" '[{"name":"product_sk","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"product_name","dataType":"VARCHAR"},{"name":"product_type","dataType":"VARCHAR"},{"name":"interest_rate","dataType":"DECIMAL"},{"name":"is_active","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_employee" "SCD Type 1 - Employee dimension" '[{"name":"employee_sk","dataType":"BIGINT"},{"name":"employee_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"position","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"hire_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_card" "SCD Type 1 - Card dimension" '[{"name":"card_sk","dataType":"BIGINT"},{"name":"card_id","dataType":"BIGINT"},{"name":"card_no_masked","dataType":"VARCHAR"},{"name":"customer_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"card_type","dataType":"VARCHAR"},{"name":"card_brand","dataType":"VARCHAR"},{"name":"credit_limit","dataType":"DECIMAL"},{"name":"issue_date","dataType":"DATE"},{"name":"expiry_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_device" "SCD Type 1 - Device dimension" '[{"name":"device_sk","dataType":"BIGINT"},{"name":"device_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"device_type","dataType":"VARCHAR"},{"name":"device_name","dataType":"VARCHAR"},{"name":"os_version","dataType":"VARCHAR"},{"name":"app_version","dataType":"VARCHAR"},{"name":"last_login","dataType":"TIMESTAMP"},{"name":"is_active","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_location" "SCD Type 1 - Location dimension" '[{"name":"location_sk","dataType":"BIGINT"},{"name":"location_id","dataType":"BIGINT"},{"name":"merchant_name","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"latitude","dataType":"DECIMAL"},{"name":"longitude","dataType":"DECIMAL"},{"name":"mcc_code","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_customer" "SCD Type 2 - Customer dimension (history)" '[{"name":"customer_sk","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"cccd","dataType":"VARCHAR"},{"name":"full_name","dataType":"VARCHAR"},{"name":"gender","dataType":"VARCHAR"},{"name":"date_of_birth","dataType":"DATE"},{"name":"phone","dataType":"VARCHAR"},{"name":"email","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"district","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"kyc_status","dataType":"VARCHAR"},{"name":"register_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"effective_from","dataType":"DATE"},{"name":"effective_to","dataType":"DATE"},{"name":"is_current","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "dim_account" "SCD Type 2 - Account dimension (history)" '[{"name":"account_sk","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"account_no","dataType":"VARCHAR"},{"name":"customer_id","dataType":"BIGINT"},{"name":"product_code","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"account_type","dataType":"VARCHAR"},{"name":"currency","dataType":"VARCHAR"},{"name":"balance","dataType":"DECIMAL"},{"name":"open_date","dataType":"DATE"},{"name":"close_date","dataType":"DATE"},{"name":"status","dataType":"VARCHAR"},{"name":"effective_from","dataType":"DATE"},{"name":"effective_to","dataType":"DATE"},{"name":"is_current","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

# Silver - Facts
register_table "silver" "fact_txn_account" "Daily account transaction facts" '[{"name":"txn_sk","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"txn_date","dataType":"TIMESTAMP"},{"name":"description","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"counter_account","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "fact_card_txn" "Daily card transaction facts" '[{"name":"txn_sk","dataType":"BIGINT"},{"name":"card_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"txn_amount","dataType":"DECIMAL"},{"name":"txn_type","dataType":"VARCHAR"},{"name":"currency","dataType":"VARCHAR"},{"name":"merchant_name","dataType":"VARCHAR"},{"name":"merchant_category","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"txn_date","dataType":"TIMESTAMP"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "fact_crm_interaction" "Daily CRM interaction facts" '[{"name":"interaction_sk","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"interaction_type","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"subject","dataType":"VARCHAR"},{"name":"description","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"created_at","dataType":"TIMESTAMP"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "fact_online_transaction" "Daily online transaction facts" '[{"name":"txn_sk","dataType":"BIGINT"},{"name":"transaction_id","dataType":"BIGINT"},{"name":"account_id","dataType":"BIGINT"},{"name":"device_id","dataType":"BIGINT"},{"name":"location_id","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"transaction_type","dataType":"VARCHAR"},{"name":"channel","dataType":"VARCHAR"},{"name":"amount","dataType":"DECIMAL"},{"name":"currency","dataType":"VARCHAR"},{"name":"is_fraud","dataType":"VARCHAR"},{"name":"fraud_reason","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"transaction_date","dataType":"TIMESTAMP"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "silver" "fact_support_ticket" "Daily support ticket facts" '[{"name":"ticket_sk","dataType":"BIGINT"},{"name":"customer_id","dataType":"BIGINT"},{"name":"ticket_type","dataType":"VARCHAR"},{"name":"priority","dataType":"VARCHAR"},{"name":"status","dataType":"VARCHAR"},{"name":"subject","dataType":"VARCHAR"},{"name":"description","dataType":"VARCHAR"},{"name":"created_at","dataType":"TIMESTAMP"},{"name":"resolved_at","dataType":"TIMESTAMP"},{"name":"cob_dt","dataType":"DATE"}]'

echo ""
echo "Registering GOLD tables..."
echo "========================="

# Gold - History tables
register_table "gold" "mart_customer_360" "Customer 360 history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"gender","dataType":"VARCHAR"},{"name":"date_of_birth","dataType":"DATE"},{"name":"phone","dataType":"VARCHAR"},{"name":"email","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"district","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"kyc_status","dataType":"VARCHAR"},{"name":"register_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"total_accounts","dataType":"INT"},{"name":"total_cards","dataType":"INT"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "rfm_segment" "RFM segmentation history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"recency_days","dataType":"INT"},{"name":"frequency","dataType":"INT"},{"name":"monetary","dataType":"DECIMAL"},{"name":"rfm_score","dataType":"INT"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "churn_prediction" "Churn prediction history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"churn_probability","dataType":"DECIMAL"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"last_transaction_date","dataType":"DATE"},{"name":"days_since_last_txn","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "campaign_target" "Campaign targeting history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"campaign_type","dataType":"VARCHAR"},{"name":"priority_score","dataType":"DECIMAL"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "cross_sell_segment" "Cross-sell segmentation history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"product_ownership","dataType":"VARCHAR"},{"name":"cross_sell_segment","dataType":"VARCHAR"},{"name":"recommended_products","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_balance_summary" "Customer balance summary history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"savings_balance","dataType":"DECIMAL"},{"name":"checking_balance","dataType":"DECIMAL"},{"name":"loan_balance","dataType":"DECIMAL"},{"name":"aum_bucket","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_transaction_summary" "Customer transaction summary history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_transactions","dataType":"INT"},{"name":"total_amount","dataType":"DECIMAL"},{"name":"avg_transaction_amount","dataType":"DECIMAL"},{"name":"debit_count","dataType":"INT"},{"name":"credit_count","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_product_summary" "Customer product summary history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_accounts","dataType":"INT"},{"name":"total_cards","dataType":"INT"},{"name":"total_loans","dataType":"INT"},{"name":"product_count","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_card_summary" "Customer card summary history" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_cards","dataType":"INT"},{"name":"total_credit_limit","dataType":"DECIMAL"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"avg_txn_amount","dataType":"DECIMAL"},{"name":"cob_dt","dataType":"DATE"}]'

# Gold - Current tables
register_table "gold" "mart_customer_360_current" "Customer 360 current snapshot" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"gender","dataType":"VARCHAR"},{"name":"date_of_birth","dataType":"DATE"},{"name":"phone","dataType":"VARCHAR"},{"name":"email","dataType":"VARCHAR"},{"name":"address","dataType":"VARCHAR"},{"name":"city","dataType":"VARCHAR"},{"name":"district","dataType":"VARCHAR"},{"name":"branch_code","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"kyc_status","dataType":"VARCHAR"},{"name":"register_date","dataType":"DATE"},{"name":"is_active","dataType":"INT"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"total_accounts","dataType":"INT"},{"name":"total_cards","dataType":"INT"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "rfm_segment_current" "RFM segmentation current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"recency_days","dataType":"INT"},{"name":"frequency","dataType":"INT"},{"name":"monetary","dataType":"DECIMAL"},{"name":"rfm_score","dataType":"INT"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "churn_prediction_current" "Churn prediction current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"churn_probability","dataType":"DECIMAL"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"last_transaction_date","dataType":"DATE"},{"name":"days_since_last_txn","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "campaign_target_current" "Campaign targeting current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"customer_segment","dataType":"VARCHAR"},{"name":"rfm_segment","dataType":"VARCHAR"},{"name":"churn_risk","dataType":"VARCHAR"},{"name":"campaign_type","dataType":"VARCHAR"},{"name":"priority_score","dataType":"DECIMAL"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "cross_sell_segment_current" "Cross-sell segmentation current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"product_ownership","dataType":"VARCHAR"},{"name":"cross_sell_segment","dataType":"VARCHAR"},{"name":"recommended_products","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_balance_summary_current" "Customer balance summary current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"savings_balance","dataType":"DECIMAL"},{"name":"checking_balance","dataType":"DECIMAL"},{"name":"loan_balance","dataType":"DECIMAL"},{"name":"aum_bucket","dataType":"VARCHAR"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_transaction_summary_current" "Customer transaction summary current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_transactions","dataType":"INT"},{"name":"total_amount","dataType":"DECIMAL"},{"name":"avg_transaction_amount","dataType":"DECIMAL"},{"name":"debit_count","dataType":"INT"},{"name":"credit_count","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_product_summary_current" "Customer product summary current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_accounts","dataType":"INT"},{"name":"total_cards","dataType":"INT"},{"name":"total_loans","dataType":"INT"},{"name":"product_count","dataType":"INT"},{"name":"cob_dt","dataType":"DATE"}]'

register_table "gold" "customer_card_summary_current" "Customer card summary current" '[{"name":"customer_id","dataType":"BIGINT"},{"name":"full_name","dataType":"VARCHAR"},{"name":"total_cards","dataType":"INT"},{"name":"total_credit_limit","dataType":"DECIMAL"},{"name":"total_balance","dataType":"DECIMAL"},{"name":"avg_txn_amount","dataType":"DECIMAL"},{"name":"cob_dt","dataType":"DATE"}]'

echo ""
echo "=========================="
echo "Registration complete!"
echo "=========================="
