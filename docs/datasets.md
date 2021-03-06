
# Dataset descriptions

- Elliptic Dataset
- FinCEN Dataset
- Taiwanese Default Dataset
- country-codes Dataset

## Elliptic Dataset

This dataset contains real bitcoin transactions. The total amount of transactions is 46 564.

To showcase the benefits of active learning, we add a cost of labelling to every transaction. The cost of one work minute is set to 12.5 NOK, but we model two different scenarios for the labelling processses dependence on outcome.

1. **simplisitc**: Every transaction requires the same amount of time Gaussian(mean=5,sd=1)
2. **realistic**: Illicit transactions are more time consuming. Licit transactions are modelled as a Gaussian(mean=5,sd=5,1) distribution, while illicit are modelled as a Gaussian(mean=10,sd=1.5) distribution.

The fields *simple_kr* and *real_kr* are generated and added to the dataset, which then contains the following fields:

- **txId** (Integer). A unique id
- **class** (Integer). Is the transaction illicit? All unknown outcomes have been removed in our *clean* dataset.
- **time** (Integer). A time index between 1 and 49. This is used for train/test splits in several notebooks (f.e 1-34 train, 35-49 test).
- **V1-V167** (Double standardized - between 0 and 1) . Features associated with transactions. The first 94 represents local information regarding the transaction, and the remaining are common features.

- **simple_kr** (Double) As described above
- **real_kr** (Double) As described above

Here is an example:

![image.png](/docs/images/elliptic.png)

The dataset source is [Kaggle](https://www.kaggle.com/ellipticco/elliptic-data-set).

The processed datasets are available at ```public/clean/elliptic```

## FinCEN Dataset

This dataset contains information about more than 35 billion transactions from 2000 to 2017, reported as suspicious by financial institutions. There are a total of 4501 transactions. Every transaction contains the following.

- **id**: Transaction identification number generated by ICIJ
- **filer**: Financial institution that filed the report with FinCEN
- **begin_date**: Date the first transaction in the reported transaction by the filer (set of transactions with the same originator and beneficiary) took place
- **end_date**: Date the last transaction in the reported transaction by the filer (se
t of transactions with same originator and beneficiary) took place
- **sender**: Bank where the transaction (s) was originated
- **sender_country**: Location country of the originator bank
- **sender_iso**: Originator bank ISO code of the bank location country
- **beneficiary**: Bank where the transaction (s) was received
- **beneficiary_country**: Location country of the beneficiary bank
- **beneficiary_ISO**: Beneficiary bank ISO code of the bank location country
- **number_transactions**: Number of transactions
- **amount_transactions**: Total amount of the transactions

![image.png](/docs/images/fincen.png)

The dataset source is [Kaggle](https://www.kaggle.com/pallaviroyal/the-fincen-files).

The processed datasets are available at  ```public/clean/fincen```

## Taiwanese default dataset

Referred to as *doccc* - default of credit card clients.

Classification dataset with the following variables (copied from source):

- **Y:** Response variable. Default payment (Yes = 1, No = 0)
- **X1:** Amount of the given credit (NT dollar): it includes both the individual consumer credit and his/her family (supplementary) credit.
- **X2:** Gender (1 = male; 2 = female).
- **X3:** Education (1 = graduate school; 2 = university; 3 = high school; 4 = others).
- **X4:** Marital status (1 = married; 2 = single; 3 = others).
- **X5:** Age (year).
- **X6 - X11:** History of past payment. We tracked the past monthly payment records (from April to September, 2005) as follows: X6 = the repayment status in September, 2005; X7 = the repayment status in August, 2005; . . .;X11 = the repayment status in April, 2005. The measurement scale for the repayment status is: -1 = pay duly; 1 = payment delay for one month; 2 = payment delay for two months; . . .; 8 = payment delay for eight months; 9 = payment delay for nine months and above.
- **X12-X17:** Amount of bill statement (NT dollar). X12 = amount of bill statement in September, 2005; X13 = amount of bill statement in August, 2005; . . .; X17 = amount of bill statement in April, 2005.
- **X18-X23:** Amount of previous payment (NT dollar). X18 = amount paid in September, 2005; X19 = amount paid in August, 2005; . . .;X23 = amount paid in April, 2005.

The dataset contains 30 000 observations.

In our flow, the dataset from the [source](https://archive.ics.uci.edu/ml/machine-learning-databases/00350/) is parsed to bronze, and casted as correct types and split into to tables consisting of the dataset itself and a description table in silver.

The silver datasets are available at ```public/silver/doccc```

## Country-codes dataset

This dataset contains international nomennclature, iso codes and geographic coordinates all countries of the world. It is used for the creation of a shiny app displaying the suspicious transactions between banks across the glob through a GNN model.

Features:

- **OFFICIAL LANG CODE:** Official language code (EN, FR, SP, ...)
- **ISO2 CODE**
- **ISO3 CODE**
- **ONU CODE**
- **IS ILOMEMBER:** binay (Y/N)
- **IS RECEIVING QUEST:** binay (Y/N)
- **LABEL EN** english name
- **LABEL FR** Franch name
- **LABEL SP** Spanish name
- **Geo Shape** Multipolygon coordinates
- **geo_point_2d** Capital coordinates (long, lat)

![image.png](/docs/images/countries_code1.png)
![image.png](/docs/images/countries_code2.png)

 The data set is available at ```public/tools```
