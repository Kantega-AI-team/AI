
# Beskrivelse av datasett

- Elliptic Dataset
- Synthetic Transactions Dataset
- FinCEN Dataset
- Taiwanese Default Dataset

## Elliptic Dataset

Elliptic Datasettet innholder ekte Bitcointransksjoner. Det totale antallet transaksjoner er 46564. Hver transaksjon har følgende features:

1. **txId** [Integer]. Unik ID for hver transaskjon.

2. **time** [Integer]. Fra 1 til 49. Generelt brukes den til å dele dataene mellom train og test set ( f.e 1-34 train, 35-49 test)

3. **Features** (Numeric standardized - between 0 and 1) . 166 features tilknyttet hver transaksjon. Den første 94 representerer lokal informasjon om transaksjonen og de resterende 72 er samlet features. Dessverre er det ikke av noen av disse 166 features.

4. **class** (binary). Variable som beskriver om en transaksajon er lovlig eller ulovlig. 1 står for ulovlig og 0 for lovlig.

Fordi vi for tiden jobber i en supervised innstilling, har jeg fjernet alle transaksjonene med Unknown class.

For å gi kundene innsikt i de økonomiske fordelene ved å bruke maskinlæringmetoder sammen med aktiv læring legger vi til informasjon om kostnadene ved merking av transkasjonene. Kostand for et minuttverk er 12.5 kr men vi antar 2 forskjellige scenarier basert på hvor lang tid det tar å merke en transaksjon:

- **simplisitc**: Alle transaksjoner tar omtrent samme tid Gaussian(mean=5,sd=1)

- **realistic**: Illicit transaksjoner tar mer tid enn licit transaksjoner. Licit –> Gaussian(mean=5,sd=5,1), illicit –> Gaussian(mean=10,sd=1.5))

Kolonner simple_kr og real_kr er deretter generert og lagt til datasettet.

Her er det et eksampel på datasettet

![image.png](/docs/images/elliptic.png)

Før du bruker ML-modeller husker å fjerne features “txId”, “time”, “simple_kr” og “real_kr”.

Plasseringen av datasettet er på storage account _kantageaamlaidls_ --> container _public_ --> directory clean/elliptic.

## Synthetic Financial Dataset

Dette datasettet inneholder simulerte mobil pengertransaksjoner basert på et utv
alg av “real” transaksjoner fra en mobil pengetjeneste implementert i afrikanske
 land. Hver transaksjon har følgende features:

1. **step** [Integer]: maps a unit of time in the real world. In this case 1 step is 1
hour of time. Total steps 744 (30 days simulation).
2. **type** [String]: CASH-IN, CASH-OUT, DEBIT, PAYMENT and TRANSFER.
3. **amount**: amount of the transaction in local currency.
4. **nameOrig** [String]: customer ID who started the transaction
5. **newbalanceOrig** [Numeric]: new balance after the transaction
6. **nameDest** [String]: customer ID who is the recipient of the transaction
7. **oldbalanceDest** [Numeric]: initial balance recipient before the transaction. Note
 that there is not information for customers that start with M (Merchants).
newbalanceDest [Numeric] new balance recipient after the transaction. Note that
there is not information for customers that start with M (Merchants).
8. **isFraud** (Binary): This is the transactions made by the fraudulent agents inside
the simulation. In this specific dataset the fraudulent behavior of the agents a
ims to profit by taking control or customers accounts and try to empty the funds
 by transferring to another account and then cashing out of the system.

I det originale datasettet er det også inkludert følgende features:

- **oldbalanceOrg**: initial balance before the transaction
- **isFlaggedFraud**: The business model aims to control massive transfers from one ac
count to another and flags illegal attempts. An illegal attempt in this dataset
is an attempt to transfer more than 200.000 in a single transaction.
Imidlertig, etter å ha lest en diskusjon om kaggle  om disse dataene, er jeg sle
ttet dem.

Også for “Merchant” kunder er ikke pengerbalansen tilgjengelig; derfor fjernet j
eg alle transaksjoner som involvere “Merchants” (ignen fraud begås av dem, så vi
 mister lite informasjon).

I det originale datasettet er det 6.36 millioner transaksjoner. 8213 av disse er
 fraud. Kanskje vi kan antar 3 forskjellige scenarier:

1% fraud og 99% licit (totalt 821300 med 8213 frauds)
5% fraud og 95% licit (totalt 164260 med 8213 frauds)
10% fraud og 90% licit (totalt 164260 med 8213 frauds)
Antall forskjelllige kunder som utfører svindel er 8213; derfore begås ikke 2 sv
indel av sammen kunde. det virker trygt da for å velge licit transaksjoner tilfe
ldig.

I tillegg er jeg lagt til i hvert datasett Kolonner simple_kr og real_kr generer
t some beskrevet ovenfor.

Her er eksempler på scenario a datasettet
![image.png](/docs/images/syntetic1.png)

Her er eksempler på scenario b datasettet
![image.png](/docs/images/syntetic2.png)

Her er eksempler på scenario c datasettet
![image.png](/docs/images/syntetic3.png)

Datasettene ligger tilgjengelig i data lake under ```public/clean/syntetic```

## FinCEN Dataset

Dataene inneholder informasjon om mer enn 35 milliarder dollar i transaksjoner d
atert 2000-2017 som ble rapportert av finansinstitusjoner som mistenkelige for a
merikanske myndigheter. Totalt er det 4501 transaksjoner. Hver transaksjon har f
ølgende features:

1. **id**: transaction identification number generated by ICIJ
2. **filer**: financial institution that filed the report with FinCEN
3. **begin_date**: date the first transaction in the reported transaction by the filer
(set of transactions with the same originator and beneficiary) took place
4. **end_date**: date the last transaction in the reported transaction by the filer (se
t of transactions with same originator and beneficiary) took place
5. **sender**: bank where the transaction (s) was originated
6. **sender_country**: location country of the originator bank
7. **sender_iso**: originator bank ISO code of the bank location country
8. **beneficiary**: bank where the transaction (s) was received
9. **beneficiary_country**: location country of the beneficiary bank
10. **beneficiary_ISO**: beneficiary bank ISO code of the bank location country
11. **number_transactions**: number of transactions
12. **amount_transactions**: total amount of the transactions

![image.png](/docs/images/fincen.png)

Datasettene ligger tilgjengelig i data lake under ```public/clean/fincen```


## Taiwanese default dataset

Også referert til som *doccc* - default of credit card clients.

Klassifiseringdatasett for hvorvidt en regning vil bli betalt eller ikke.

Hentet direkte fra kilden:

- Y: Response variable. Default payment (Yes = 1, No = 0)
- X1: Amount of the given credit (NT dollar): it includes both the individual consumer credit and his/her family (supplementary) credit.
- X2: Gender (1 = male; 2 = female).
- X3: Education (1 = graduate school; 2 = university; 3 = high school; 4 = others).
- X4: Marital status (1 = married; 2 = single; 3 = others).
- X5: Age (year).
- X6 - X11: History of past payment. We tracked the past monthly payment records (from April to September, 2005) as follows: X6 = the repayment status in September, 2005; X7 = the repayment status in August, 2005; . . .;X11 = the repayment status in April, 2005. The measurement scale for the repayment status is: -1 = pay duly; 1 = payment delay for one month; 2 = payment delay for two months; . . .; 8 = payment delay for eight months; 9 = payment delay for nine months and above.
- X12-X17: Amount of bill statement (NT dollar). X12 = amount of bill statement in September, 2005; X13 = amount of bill statement in August, 2005; . . .; X17 = amount of bill statement in April, 2005.
- X18-X23: Amount of previous payment (NT dollar). X18 = amount paid in September, 2005; X19 = amount paid in August, 2005; . . .;X23 = amount paid in April, 2005.

Datasettet inneholder 30 000 observasjoner.

I vår flyt er alle data hentet fra [kilden](https://archive.ics.uci.edu/ml/machine-learning-databases/00350/), via bronze, til riktig typecast og to filtrerte tabeller med data og beskrivelse i silver.

Videre etter silver bruker vi datasettet som utgangspunkt for "mock" streams til gold - delvis henting av datasettet med inkrementelle appends, enten manuelt eller automatisk over et gitt tidsrom. Se koden under [notebooks/doccc](notebooks/doccc) for detaljer
