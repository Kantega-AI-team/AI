import click
import mlflow
import mlflow.xgboost
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import f1_score, recall_score

@click.command()
@click.option("--data_path", default="/mnt/public/clean/elliptic", type=str)
@click.option("--max_depth", default=6, type=int)
def mlflow_xgb(data_path, max_depth):
    with mlflow.start_run() as run:
        # Import data
        df = DeltaTable("/dbfs"+data_path).to_pandas().drop(columns=["txId", "simple_kr", "real_kr"])
        X, y = df.drop(columns=["class"]), df[["class", "time"]]
        X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]
        X_train = X_train.drop(columns=["time"])
        X_test = X_test.drop(columns=["time"])
        y_train, y_test = y.loc[(X["time"] <= 34)], y.loc[(X["time"] > 34)]
        y_train = y_train.drop(columns=["time"])
        y_train = y_train.values.ravel().astype(int)
        y_test = y_test.drop(columns=["time"])
        y_test = y_test.values.ravel().astype(int)
        # Fit model
        xgb_classifier = xgb.XGBClassifier(max_depth=max_depth, verbosity=0)
        xgb_classifier.fit(X_train, y_train)

        # Make predictions
        predictions = xgb_classifier.predict(X_test)

        # Log param
        mlflow.log_param("max_depth", max_depth)

        # Log metrics
        f1score_xgb = f1_score(y_test, predictions)                                               
        mlflow.log_metric("f1", f1score_xgb)
        recall_xgb = recall_score(y_test, predictions)
        mlflow.log_metric("recall", recall_xgb)
        
if __name__ == "__main__":
  mlflow_xgb() # Note that this does not need arguments thanks to click