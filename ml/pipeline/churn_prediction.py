import os,logging,argparse
from datetime import datetime
import mlflow,mlflow.sklearn
import numpy as np,pandas as pd
from trino.dbapi import connect
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,f1_score,roc_auc_score
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
TRACKING_URI=os.getenv("MLFLOW_TRACKING_URI","http://mlflow:5000")
FEATURES=["total_accounts","total_cards","total_loans","total_deposit_balance","total_loan_outstanding","aum_total","txn_count_30d","txn_amount_30d","days_since_last_txn","interaction_count_90d","rfm_recency_score","rfm_frequency_score","rfm_monetary_score"]
def load_features(cob_dt):
    conn=connect(host="trino",port=8080,user="ml",catalog="iceberg",schema="serving")
    cols=",".join(FEATURES)
    sql=f"SELECT customer_id,customer_segment,{cols},churn_flag FROM mart_customer_360_current WHERE cob_dt=DATE ''{cob_dt}''"
    df=pd.read_sql(sql,conn)
    conn.close()
    return df

def train(df,cob_dt):
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment("churn_prediction")

    le=LabelEncoder()
    df["customer_segment_enc"]=le.fit_transform(df["customer_segment"].astype(str))
    feature_cols=FEATURES+["customer_segment_enc"]
    X=df[feature_cols]
    y=df["churn_flag"]

    X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=42,stratify=y)

    with mlflow.start_run(run_name=f"churn_xgb_{cob_dt}"):
        params={"n_estimators":200,"max_depth":6,"learning_rate":0.1,"objective":"binary:logistic","random_state":42}
        mlflow.log_params(params)

        model=xgb.XGBClassifier(**params,use_label_encoder=False,eval_metric="logloss")
        model.fit(X_train,y_train)

        y_pred=model.predict(X_test)
        y_prob=model.predict_proba(X_test)[:,1]

        acc=accuracy_score(y_test,y_pred)
        f1=f1_score(y_test,y_pred)
        auc=roc_auc_score(y_test,y_prob)

        mlflow.log_metric("accuracy",acc)
        mlflow.log_metric("f1_score",f1)
        mlflow.log_metric("roc_auc",auc)
        mlflow.log_metric("train_size",len(X_train))
        mlflow.log_metric("test_size",len(X_test))

        mlflow.xgboost.log_model(model,"model",
            registered_model_name="churn_xgboost",
            input_example=X_test.iloc[:5])

        logging.info(f"Metrics - accuracy:{acc:.4f} f1:{f1:.4f} roc_auc:{auc:.4f}")
    return model,{"accuracy":acc,"f1_score":f1,"roc_auc":auc}

def predict(model,df):
    le=LabelEncoder()
    df["customer_segment_enc"]=le.fit_transform(df["customer_segment"].astype(str))
    feature_cols=FEATURES+["customer_segment_enc"]
    X=df[feature_cols]
    df["churn_probability"]=model.predict_proba(X)[:,1]
    df["churn_prediction"]=model.predict(X)
    return df[["customer_id","churn_prediction","churn_probability"]].sort_values("churn_probability",ascending=False)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    parser=argparse.ArgumentParser(description="Churn prediction pipeline")
    parser.add_argument("--cob_dt",required=True,help="Business date YYYY-MM-DD")
    args=parser.parse_args()

    logging.info(f"Loading features for cob_dt={args.cob_dt}")
    df=load_features(args.cob_dt)
    logging.info(f"Loaded {len(df)} rows, {df['churn_flag'].sum()} churners")

    logging.info("Training churn model...")
    model,metrics=train(df,args.cob_dt)

    preds=predict(model,df)
    logging.info(f"Top 10 at-risk customers:")
    print(preds.head(10).to_string(index=False))

    print(f"\n=== Churn Prediction Results ===")
    print(f"Date: {args.cob_dt}")
    print(f"Rows: {len(df)}")
    print(f"Accuracy:  {metrics['accuracy']:.4f}")
    print(f"F1 Score:  {metrics['f1_score']:.4f}")
    print(f"ROC AUC:   {metrics['roc_auc']:.4f}")
    print(f"Model:     churn_xgboost (registered)")
    print(f"MLflow:    {TRACKING_URI}")
