# Databricks notebook source
# MAGIC %md
# MAGIC # Info
# MAGIC Denne notebooken bruker datasettet `labelled_elliptic.csv`, lager referansemodell med XGBoost, for deretter å matche opp mot **pool based active learning**. Til slutt brukes **SHAP** til forklarbarhet på enkeltprediksjoner

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
import xgboost as xgb
from modAL.models import ActiveLearner
from modAL.uncertainty import uncertainty_sampling
from sklearn.metrics import f1_score, recall_score

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Henter data

# COMMAND ----------

# Henter datasett fra lake
sparkdf = spark.read.format("delta").load("/mnt/public/clean/elliptic")

# COMMAND ----------

# Konverterer til pandas DF
initial_df = sparkdf.toPandas()

# COMMAND ----------

# Sjekker at data er lest inn riktig
initial_df.head()

# COMMAND ----------

# Ser på størrelsen av datasettet
initial_df.shape

# COMMAND ----------

# Teller antall licit og illicit observasjoner
initial_df["class"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ordner data for modellering

# COMMAND ----------

# For å holde innlest data rent lager jeg et subset. Her dropper jeg tre variabler fra rådata
subset = initial_df.drop(columns=["txId", "simple_kr", "real_kr"])

# COMMAND ----------

# Splitter subset i to nye data frames. En DF for uavhengige variabler og en for avhengig variabel
X, y = subset.drop(columns=["class"]), subset[["class", "time"]]

# COMMAND ----------

# Splitter i test og treningssett sånn som de gjør i artikkelen
X_train, X_test = X.loc[(X["time"] <= 34)], X.loc[(X["time"] > 34)]

# COMMAND ----------

# Dropper time-variabelen fra datasettet
X_train = X_train.drop(columns=["time"])
X_test = X_test.drop(columns=["time"])

# COMMAND ----------

# Splitter på samme måte som for de uavhengige variablene
y_train, y_test = y.loc[(X["time"] <= 34)], y.loc[(X["time"] > 34)]

# COMMAND ----------

# Dropper time og transformerer avhengig variabel til array
y_train = y_train.drop(columns=["time"])
y_train = y_train.values.ravel().astype(int)
y_test = y_test.drop(columns=["time"])
y_test = y_test.values.ravel().astype(int)

# COMMAND ----------

# MAGIC %md
# MAGIC # Etablerer baseline for modeller

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline XGB

# COMMAND ----------

# Definerer en XGBoost modell med default hyperparametere
xgb_classifier = xgb.XGBClassifier()

# Trener modellen
xgb_classifier.fit(X_train, y_train)

# COMMAND ----------

# Lager prediksjoner med test-settet
y_pred_xgb = xgb_classifier.predict(X_test)

# COMMAND ----------

# Beregner F1-score og printer resultatet
f1score_xgb = f1_score(y_test, y_pred_xgb)
print("F1-score for XGB: {acc:0.4f}".format(acc=f1score_xgb))

# COMMAND ----------

# Beregner recall score og printer resultatet
recall_xgb = recall_score(y_test, y_pred_xgb)
print("Recall-score for XGB: {acc:0.4f}".format(acc=recall_xgb))

# COMMAND ----------

# MAGIC %md
# MAGIC # Pool based uncertanty sample active learning

# COMMAND ----------

# MAGIC %md
# MAGIC ### XGB

# COMMAND ----------

# MAGIC %md
# MAGIC **Konvertering av df til numpy**

# COMMAND ----------

# Konverterer X_train og X_test til array
X_raw = X_train.to_numpy()
y_raw = y_train

X_test_al = X_test.to_numpy()
y_test_al = y_test

# COMMAND ----------

# MAGIC %md
# MAGIC **Trekker ut en sample til treningssett, resten legges i pool**

# COMMAND ----------

# Trekker ut en tilfeldig observasjon fra treningssettet som er den første observasjonen vi trener modellen med
n_labeled_examples = X_raw.shape[0]
training_indices = np.random.randint(low=0, high=n_labeled_examples + 1, size=1)

# Putter observasjonen i treningssettet for active learning
X_train_al = X_raw[training_indices]
y_train_al = y_raw[training_indices]

# Putter de resterende observasjonene fra treningssettet i en pool
X_pool = np.delete(X_raw, training_indices, axis=0)
y_pool = np.delete(y_raw, training_indices, axis=0)

# COMMAND ----------

# MAGIC %md
# MAGIC **Lager estimator for learner**

# COMMAND ----------

# Definerer learneren vi skal bruke og trener på første observasjon
xg_boost = xgb.XGBClassifier()
learner = ActiveLearner(
    estimator=xg_boost,
    query_strategy=uncertainty_sampling,
    X_training=X_train_al,
    y_training=y_train_al,
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Predikerer med modell laget på første eksempel**

# COMMAND ----------

# Predikerer med modell laget på første eksempel
predictions_xgb = learner.predict(X_test_al)

# Lagrer og printer F1-score for første itterasjon
unqueried_f1_xgb = f1_score(y_test_al, predictions_xgb)
print("Initial F1-score for XGB: {acc:0.4f}".format(acc=unqueried_f1_xgb))

# COMMAND ----------

# MAGIC %md
# MAGIC **Oppdaterer modellen med pool based sampling**

# COMMAND ----------

# Definerer hvor mange itterasjoner vi ønsker at modellen skal gjøre
# og lager en variabel for å spare på F1-score for hver itterasjon
N_QUERIES = 1500
performance_history_xgb = [unqueried_f1_xgb]


# Looper igjennom antall itterasjoner som er satt og henter nye observasjoner
# fra poolen våres ved å bruke uncertainty sampling.
for index in range(N_QUERIES):
    query_index, query_instance = learner.query(X_pool)

    # Oppdaterer modellen med den nye observasjonen fra poolen
    X_nq, y_nq = X_pool[query_index].reshape(1, -1), y_pool[query_index].reshape(
        1,
    )
    learner.teach(X=X_nq, y=y_nq)

    # Fjerner observasjonen fra poolen
    X_pool, y_pool = np.delete(X_pool, query_index, axis=0), np.delete(
        y_pool, query_index
    )

    # Beragner F1-score og printer denne
    pred = learner.predict(X_test_al)
    model_f1 = f1_score(y_test_al, pred)
    print("F1 after query {n}: {acc:0.4f}".format(n=index + 1, acc=model_f1))

    # Lagrer F1-score for itterasjonen
    performance_history_xgb.append(model_f1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plott av F1-score

# COMMAND ----------

fig, ax = plt.subplots(figsize=(8.5, 6), dpi=130)

# Plott av F1-score for referansemodell
ax.axhline(y=f1score_xgb, color="r", linestyle="--")
# Plott av F1-score for active learner
ax.plot(performance_history_xgb, color="b", linestyle="-")

ax.set_ylim(bottom=0, top=1)
ax.grid(True)

ax.set_title("Incremental F1-score")
ax.set_xlabel("Query iteration")
ax.set_ylabel("F1-score")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # SHAP values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lager explainer med modell fra active learner

# COMMAND ----------

shap_explainer = shap.TreeExplainer(xg_boost)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualiserer enkeltprediksjoner fra testsettet

# COMMAND ----------

# Ekte utfallet til observasjonen
y_test[0]

# COMMAND ----------

# Shap-verdier og predikert utfall av observasjon
choosen_instance = X_test.iloc[[0]]
shap_values = shap_explainer.shap_values(choosen_instance)
shap.initjs()
shap.force_plot(
    shap_explainer.expected_value, shap_values, choosen_instance, link="logit"
)

# COMMAND ----------

# Ekte utfallet til observasjonen
y_test[4]

# COMMAND ----------

# Shap-verdier og predikert utfall av observasjon
choosen_instance = X_test.iloc[[4]]
shap_values = shap_explainer.shap_values(choosen_instance)
shap.initjs()
shap.force_plot(
    shap_explainer.expected_value, shap_values, choosen_instance, link="logit"
)
