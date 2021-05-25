# Databricks notebook source
#load required libraries

library(SparkR)
library(dplyr)
library(tibble)
library(truncnorm)
library(stringr)


# COMMAND ----------

#load original dataset - Note: header is TRUE

# databricks dataset
 data_orig_db <- read.df(source = "csv",
                         path = paste0("/mnt/public/raw/fincen/",
                                       "FinCEN_dataset_original.csv"),
                         header = "true", inferSchema = "true",
                         delimiter = ",")

# transform data_orig_db in  r data frame
data_final <- as.data.frame(data_orig_db)


# keep only useful features
data_final <- subset(data_final, select = c(id, filer_org_name, begin_date,
                                            end_date, originator_bank,
                                            originator_bank_country,
                                            originator_iso, beneficiary_bank,
                                            beneficiary_bank_country,
                                            beneficiary_iso,
                                            number_transactions,
                                            amount_transactions))

# modify the name of the columns

colnames(data_final) <- c("id", "filer", "begin_date", "end_date", "sender",
                          "sender_country", "sender_iso", "beneficiary",
                          "beneficiary_country", "beneficiary_iso",
                          "number_transactions", "amount_transactions")


# COMMAND ----------

# analyse/clean the names of the banks (string manipulation)


# delete all punctuation marks from sender and beneficiary bank

data_final$sender <- gsub(pattern = "[[:punct:]]", replacement = "",
                          data_final$sender)

data_final$beneficiary <- gsub(pattern = "[[:punct:]]", replacement = "",
                               data_final$beneficiary)


# transform all letters in upper case for sender and beneficiary bank

data_final$sender <- toupper(data_final$sender)

data_final$beneficiary <- toupper(data_final$beneficiary)


#remove whitespace from start and end of the strings

data_final$sender <- str_trim(data_final$sender)

data_final$beneficiary <- str_trim(data_final$beneficiary)

## reduce repeated spaces inside the strings

data_final$sender <- str_squish(data_final$sender)

data_final$beneficiary <- str_squish(data_final$beneficiary)


# COMMAND ----------

#manual modifications

data_final$sender[which(data_final$sender == "AS EXPO BANK")] <- "AS EXPOBANK"
data_final$beneficiary[
  which(data_final$beneficiary == "AS EXPO BANK")] <- "AS EXPOBANK"

data_final$sender[
  which(data_final$sender == "AIZKRAUKLES")] <- "AIZKRAUKLES BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "AIZKRAUKLES")] <- "AIZKRAUKLES BANKA"

data_final$sender[
  which(data_final$sender == "AIZRAULKLES BANKA")] <- "AIZKRAUKLES BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "AIZRAULKLES BANKA")] <- "AIZKRAUKLES BANKA"

data_final$sender[which(data_final$sender == "ALFABANK")] <- "ALFA BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "ALFABANK")] <- "ALFA BANK"

data_final$sender[
  which(
    data_final$sender == "AMSTERDAM TRADE BANK")] <- "AMSTERDAM TRADE BANK NV"
data_final$beneficiary[
  which(
    data_final$beneficiary == "AMSTERDAM TRADE BANK"
    )] <- "AMSTERDAM TRADE BANK NV"

data_final$sender[
  which(
    data_final$sender == "ASYA KATILIM BANKASI A S"
    )] <-  "ASYA KATILIM BANKASI AS"
data_final$beneficiary[
  which(
    data_final$beneficiary == "ASYA KATILIM BANKASI A S"
    )] <-  "ASYA KATILIM BANKASI AS"

data_final$sender[
  which(
    data_final$sender == "AUSTRALIA AND NEW ZEALAND BANKING GROUP LTD"
    )] <- "AUSTRALIA AND NEW ZEALAND BANKING"
data_final$beneficiary[
  which(
    data_final$beneficiary ==  "AUSTRALIA AND NEW ZEALAND BANKING GROUP LTD"
    )] <- "AUSTRALIA AND NEW ZEALAND BANKING"

data_final$sender[
  which(
    data_final$sender == "BANCA DE ECONOMII A MOLDOVEI"
    )] <-  "BANCA DE ECONOMII"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANCA DE ECONOMII A MOLDOVEI"
    )] <-  "BANCA DE ECONOMII"

data_final$sender[
  which(
    data_final$sender == "BANGKOK BANK PUBLIC COMPANY LIMITED"
    )] <-  "BANGKOK BANK PUBLIC COMPANY"
data_final$beneficiary[
  which(
    data_final$beneficiary  == "BANGKOK BANK PUBLIC COMPANY LIMITED"
    )] <-  "BANGKOK BANK PUBLIC COMPANY"

data_final$sender[
  which(
    data_final$sender == "BANK HAPOALIM B M"
    )] <-  "BANK HAPOALIM BM"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK HAPOALIM B M")] <-  "BANK HAPOALIM BM"

data_final$sender[
  which(
    data_final$sender == "BANK HAPOALIM")] <-  "BANK HAPOALIM BM"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK HAPOALIM")] <-  "BANK HAPOALIM BM"

data_final$sender[
  which(
    data_final$sender == "BANK JULIUS BAER AND COLTD"
    )] <-  "BANK JULIUS BAER AND CO LTD"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK JULIUS BAER AND COLTD"
    )] <-  "BANK JULIUS BAER AND CO LTD"

data_final$sender[
  which(
    data_final$sender == "BANK JULIUS BAER CO LTD"
    )] <-  "BANK JULIUS BAER AND CO LTD"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK JULIUS BAER CO LTD"
    )] <-  "BANK JULIUS BAER AND CO LTD"

data_final$sender[
  which(
    data_final$sender == "BANK OF AMERICA N A")] <-  "BANK OF AMERICA NA"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK OF AMERICA N A")] <-  "BANK OF AMERICA NA"

data_final$sender[
  which(
    data_final$sender == "BANK OF COMMUNICATIONSHONG KONG"
    )] <-  "BANK OF COMMUNICATIONS"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK OF COMMUNICATIONSHONG KONG"
    )] <-  "BANK OF COMMUNICATIONS"

data_final$sender[
  which(
    data_final$sender == "BANK OF CYPRUS PUBLIC CO LTD"
    )] <-  "BANK OF CYPRUS PUBLIC COMPANY LTD"
data_final$beneficiary[
  which(
    data_final$beneficiary == "BANK OF CYPRUS PUBLIC CO LTD"
    )] <-  "BANK OF CYPRUS PUBLIC COMPANY LTD"

data_final$sender[
  which(
    data_final$sender == "BANK OF CYPRUS PUBLIC COMPANY LIMITED"
    )] <-  "BANK OF CYPRUS PUBLIC COMPANY LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "BANK OF CYPRUS PUBLIC COMPANY LIMITED"
        )] <-  "BANK OF CYPRUS PUBLIC COMPANY LTD"

data_final$sender[
  which(data_final$sender == "BANQUE DE COMMERCE ET DE"
        )] <-  "BANQUE DE COMMERCE ET DE PLACEMENTS"
data_final$beneficiary[
  which(data_final$beneficiary == "BANQUE DE COMMERCE ET DE"
        )] <-  "BANQUE DE COMMERCE ET DE PLACEMENTS"

data_final$sender[
  which(
    data_final$sender == "BANQUE DE COMMERCE ET DE PLACEMENTS S A"
    )] <-  "BANQUE DE COMMERCE ET DE PLACEMENTS SA"
data_final$beneficiary[
  which(data_final$beneficiary == "BANQUE DE COMMERCE ET DE PLACEMENTS S A"
        )] <-  "BANQUE DE COMMERCE ET DE PLACEMENTS SA"

data_final$sender[
  which(data_final$sender == "CALEDONIAN BANK LIMITED"
        )] <-  "CALEDONIAN BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "CALEDONIAN BANK LIMITED"
        )] <-  "CALEDONIAN BANK LTD"

data_final$sender[
  which(data_final$sender == "CESKOSLOVENSKA OBCHODNA BANKA AS"
        )] <-  "CESKOSLOVENSKA OBCHODNI BANKA AS"
data_final$beneficiary[
  which(data_final$beneficiary == "CESKOSLOVENSKA OBCHODNA BANKA AS"
        )] <-  "CESKOSLOVENSKA OBCHODNI BANKA AS"

data_final$sender[
  which(data_final$sender == "DANSKE BANK A S")] <-  "DANSKE BANK AS"
data_final$beneficiary[
  which(data_final$beneficiary == "DANSKE BANK A S")] <-  "DANSKE BANK AS"

data_final$sender[
  which(
    data_final$sender == "DANSKE BANK A S ESTONIA BRANCH"
    )] <-  "DANSKE BANK AS ESTONIA BRANCH"
data_final$beneficiary[
  which(data_final$beneficiary == "DANSKE BANK A S ESTONIA BRANCH"
        )] <-  "DANSKE BANK AS ESTONIA BRANCH"

data_final$sender[
  which(data_final$sender == "EESTI KREDITBAN")] <-  "EESTI KREDITBANK"
data_final$beneficiary[
  which(data_final$beneficiary == "EESTI KREDITBAN")] <-  "EESTI KREDITBANK"

data_final$sender[
  which(data_final$sender == "FBME BANK LIMITED")] <-  "FBME BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "FBME BANK LIMITED")] <-  "FBME BANK LTD"

data_final$sender[
  which(data_final$sender == "FIRST UKRANIAN INTERNATIONAL BANK"
        )] <-  "FIRST UKRAINIAN INTERNATIONAL BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "FIRST UKRANIAN INTERNATIONAL BANK"
        )] <-  "FIRST UKRAINIAN INTERNATIONAL BANK"

data_final$sender[
  which(data_final$sender == "HANG SENG BANK LIMITED"
        )] <-  "HANG SENG BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "HANG SENG BANK LIMITED"
        )] <-  "HANG SENG BANK LTD"

data_final$sender[
  which(data_final$sender == "HANG SHENG BANK")] <-  "HANG SENG BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "HANG SHENG BANK")] <-  "HANG SENG BANK"

data_final$sender[
  which(data_final$sender == "HANG SHENG BANK")] <-  "HANG SENG BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "HANG SHENG BANK")] <-  "HANG SENG BANK"

data_final$sender[
  which(data_final$sender == "HSBC")] <-  "HSBC BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "HSBC")] <-  "HSBC BANK"

data_final$sender[
  which(data_final$sender == "HSBC PRIVATE BK")] <-  "HSBC PRIVATE BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "HSBC PRIVATE BK")] <-  "HSBC PRIVATE BANK"

data_final$sender[
  which(data_final$sender == "HSBC PRIVATE BK")] <-  "HSBC PRIVATE BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "HSBC PRIVATE BK")] <-  "HSBC PRIVATE BANK"

data_final$sender[
  which(data_final$sender == "JSC NORVIKBANKA")] <-  "JSC NORVIK BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "JSC NORVIKBANKA")] <-  "JSC NORVIK BANKA"

data_final$sender[
  which(data_final$sender == "KUWAIT TURKISH PARTICIPATION BK INC"
        )] <-  "KUWAIT TURKISH PARTICIPATION BANK INC"
data_final$beneficiary[
  which(data_final$beneficiary == "KUWAIT TURKISH PARTICIPATION BK INC"
        )] <-  "KUWAIT TURKISH PARTICIPATION BANK INC"

data_final$sender[
  which(data_final$sender == "MACQUARIE BANK LIMITED"
        )] <-  "MACQUARIE BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "MACQUARIE BANK LIMITED"
        )] <-  "MACQUARIE BANK LTD"

data_final$sender[
  which(data_final$sender == "MARFIN POPULAR BANK"
        )] <-  "MARFIN POPULAR BANK PUBLIC CO LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "MARFIN POPULAR BANK"
        )] <-  "MARFIN POPULAR BANK PUBLIC CO LTD"

data_final$sender[
  which(data_final$sender == "MARFIN POPULAR BANK PCL"
        )] <-  "MARFIN POPULAR BANK PUBLIC CO LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "MARFIN POPULAR BANK PCL"
        )] <-  "MARFIN POPULAR BANK PUBLIC CO LTD"

data_final$sender[
  which(data_final$sender == "MEINL BANKAG")] <-  "MEINL BANK AG"
data_final$beneficiary[
  which(data_final$beneficiary == "MEINL BANKAG")] <-  "MEINL BANK AG"

data_final$sender[
  which(data_final$sender == "MERRILL LYNCH CO")] <-  "MERRILL LYNCH AND CO"
data_final$beneficiary[
  which(data_final$beneficiary == "MERRILL LYNCH CO"
        )] <-  "MERRILL LYNCH AND CO"

data_final$sender[
  which(data_final$sender == "MIZUHO CORPORATE BANK LIMITED"
        )] <-  "MIZUHO CORPORATE BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "MIZUHO CORPORATE BANK LIMITED"
        )] <-  "MIZUHO CORPORATE BANK"

data_final$sender[
  which(data_final$sender == "MORGAN STANLEY CO INTERNATIONAL PLC"
        )] <-  "MORGAN STANLEY CO INTL PLC"
data_final$beneficiary[
  which(data_final$beneficiary == "MORGAN STANLEY CO INTERNATIONAL PLC"
        )] <-  "MORGAN STANLEY CO INTL PLC"

data_final$sender[
  which(data_final$sender == "MORGAN STANLEYCO INC NATIONAL"
        )] <-  "MORGAN STANLEY CO INC NATIONAL"
data_final$beneficiary[
  which(data_final$beneficiary == "MORGAN STANLEYCO INC NATIONAL"
        )] <-  "MORGAN STANLEY CO INC NATIONAL"

data_final$sender[
  which(data_final$sender  == paste("NATIONAL BANK FOR FOREIGN ECONOMIC",
  "ACTIVITY OF THE REPUBLIC OF UZBEKISTAN", sep = "")
  )] <-  "NATIONAL BANK FOR FOREIGN ECONOMIC ACTIVITY OF THE REP OF UZBEKISTAN"
data_final$beneficiary[
  which(data_final$beneficiary  == paste("NATIONAL BANK FOR FOREIGN ECONOMIC",
  "ACTIVITY OF THE REPUBLIC OF UZBEKISTAN", sep = "")
  )] <-  "NATIONAL BANK FOR FOREIGN ECONOMIC ACTIVITY OF THE REP OF UZBEKISTAN"

data_final$sender[
  which(data_final$sender == "NATIONAL BANK OF RAS AL KAIMAH"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"
data_final$beneficiary[
  which(data_final$beneficiary == "NATIONAL BANK OF RAS AL KAIMAH"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"

data_final$sender[
  which(data_final$sender == "NATIONAL BANK OF RAS AL KHAIMAH PSC"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"
data_final$beneficiary[
  which(data_final$beneficiary == "NATIONAL BANK OF RAS AL KHAIMAH PSC"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"

data_final$sender[
  which(data_final$sender == "NATIONAL BANK OF RAS ALKHAIMAH"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"
data_final$beneficiary[
  which(data_final$beneficiary == "NATIONAL BANK OF RAS ALKHAIMAH"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"

data_final$sender[
  which(data_final$sender == "NATIONAL BK OF RAS AL KHAIMAH PSC"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"
data_final$beneficiary[
  which(data_final$beneficiary == "NATIONAL BK OF RAS AL KHAIMAH PSC"
        )] <-  "NATIONAL BANK OF RAS AL KHAIMAH"

data_final$sender[
  which(data_final$sender == "NORDEA BANK FINLAND PLC LONDONNORDEA BANK AB"
        )] <-  "NORDEA BANK FINLAND PLC LONDON"
data_final$beneficiary[
  which(data_final$beneficiary == "NORDEA BANK FINLAND PLC LONDONNORDEA BANK AB"
        )] <-  "NORDEA BANK FINLAND PLC LONDON"

data_final$sender[
  which(data_final$sender == "NORVIC BANKA JSC")] <-  "JSC NORVIK BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "NORVIC BANKA JSC")] <-  "JSC NORVIK BANKA"

data_final$sender[
  which(data_final$sender == "NORVIK BANKA")] <-  "JSC NORVIK BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "NORVIK BANKA")] <-  "JSC NORVIK BANKA"

data_final$sender[
  which(data_final$sender == "NORVIK BANKA JSC")] <-  "JSC NORVIK BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "NORVIK BANKA JSC")] <-  "JSC NORVIK BANKA"

data_final$sender[
  which(data_final$sender == "OCBC WING HANG BANK LIMITED"
        )] <-  "OCBC WING HANG BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "OCBC WING HANG BANK LIMITED"
        )] <-  "OCBC WING HANG BANK LTD"

data_final$sender[
  which(data_final$sender == "OVERSEA CHINESE BANKING CORP"
        )] <-  "OVERSEA CHINESE BANKING CORP LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "OVERSEA CHINESE BANKING CORP"
        )] <-  "OVERSEA CHINESE BANKING CORP LTD"

data_final$sender[
  which(data_final$sender == "PERSHING LLC US")] <-  "PERSHING LLC"
data_final$beneficiary[
  which(data_final$beneficiary == "PERSHING LLC US")] <-  "PERSHING LLC"

data_final$sender[
  which(data_final$sender == "P MORGAN CHASE BANK NATIONAL"
        )] <-  "MORGAN CHASE BANK NATIONAL"
data_final$beneficiary[
  which(data_final$beneficiary == "P MORGAN CHASE BANK NATIONAL"
        )] <-  "MORGAN CHASE BANK NATIONAL"

data_final$sender[
  which(data_final$sender == "PPF BANKA A S")] <-  "PPF BANKA AS"
data_final$beneficiary[
  which(data_final$beneficiary == "PPF BANKA A S")] <-  "PPF BANKA AS"

data_final$sender[
  which(data_final$sender == "PRIVAL BANK S A")] <-  "PRIVAL BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "PRIVAL BANK S A")] <-  "PRIVAL BANK"

data_final$sender[
  which(data_final$sender == "RAIFFEISEN ZENTRALBANKOESTERREICH AG"
        )] <-  "RAIFFEISEN ZENTRALBANK OESTERREICH AG"
data_final$beneficiary[
  which(data_final$beneficiary == "RAIFFEISEN ZENTRALBANKOESTERREICH AG"
        )] <-  "RAIFFEISEN ZENTRALBANK OESTERREICH AG"

data_final$sender[
  which(data_final$sender == "RAIFFEISENBANK")] <-  "RAIFFEISEN BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "RAIFFEISENBANK")] <-  "RAIFFEISEN BANK"

data_final$sender[
  which(data_final$sender == "RAIFFFEISEN")] <-  "RAIFFEISEN BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "RAIFFFEISEN")] <-  "RAIFFEISEN BANK"

data_final$sender[
  which(data_final$sender == "RAIFFEISENBANK ZAO")] <-  "RAIFFEISEN BANK ZAO"
data_final$beneficiary[
  which(data_final$beneficiary == "RAIFFEISENBANK ZAO"
        )] <- "RAIFFEISEN BANK ZAO"

data_final$sender[
  which(data_final$sender == "RIETUMU BANKA JSC")] <-  "RIETUMU BANKA"
data_final$beneficiary[
  which(data_final$beneficiary == "RIETUMU BANKA JSC")] <-  "RIETUMU BANKA"

data_final$sender[
  which(data_final$sender == "RIGENESIS BANK AS")] <-  "RIGENSIS BANK AS"
data_final$beneficiary[
  which(data_final$beneficiary == "RIGENESIS BANK AS")] <-  "RIGENSIS BANK AS"

data_final$sender[
  which(data_final$sender == "RUSSIAN COMMERCIAL BANK"
        )] <-  "RUSSIAN COMMERCIAL BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "RUSSIAN COMMERCIAL BANK"
        )] <-  "RUSSIAN COMMERCIAL BANK LTD"

data_final$sender[
  which(data_final$sender == "SEB BANKAS")] <-  "SEB BANK AS"
data_final$beneficiary[
  which(data_final$beneficiary == "SEB BANKAS")] <-  "SEB BANK AS"

data_final$sender[
  which(data_final$sender == "STANDARD CHARTERED BANK HONG KONG LIMITED"
        )] <-  "STANDARD CHARTERED BANK HK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "STANDARD CHARTERED BANK HONG KONG LIMITED"
        )] <-  "STANDARD CHARTERED BANK HK LTD"

data_final$sender[
  which(data_final$sender == "STANDARD CHARTERED BANK HONG KONG LTD"
        )] <-  "STANDARD CHARTERED BANK HK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "STANDARD CHARTERED BANK HONG KONG LTD"
        )] <-  "STANDARD CHARTERED BANK HK LTD"

data_final$sender[
  which(data_final$sender == "STANSARD CHARTERED BANK"
        )] <-  "STANDARD CHARTERED BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "STANSARD CHARTERED BANK"
        )] <-  "STANDARD CHARTERED BANK"

data_final$sender[
  which(data_final$sender == "STATE BANK FOR FOREIGN ECONOMIC AFF"
        )] <-  "STATE BANK FOR FOREIGN ECONOMIC AFFAIRS OF TURKMENISTAN"
data_final$beneficiary[
  which(data_final$beneficiary == "STATE BANK FOR FOREIGN ECONOMIC AFF"
        )] <-  "STATE BANK FOR FOREIGN ECONOMIC AFFAIRS OF TURKMENISTAN"

data_final$sender[
  which(data_final$sender == "TRANSCREDITBANK")] <-  "TRANSCREDIT BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "TRANSCREDITBANK")] <-  "TRANSCREDIT BANK"

data_final$sender[
  which(data_final$sender == "UNITED OVERSEAS BANK LIMITED"
        )] <-  "UNITED OVERSEAS BANK LTD"
data_final$beneficiary[
  which(data_final$beneficiary == "UNITED OVERSEAS BANK LIMITED"
        )] <-  "UNITED OVERSEAS BANK LTD"

data_final$sender[
  which(data_final$sender == "VERWALTUNGS UND PRIVATBANK AG"
        )] <-  "VERWALTUNGS UND PRIVAT BANK AG"
data_final$beneficiary[
  which(data_final$beneficiary == "VERWALTUNGS UND PRIVATBANK AG"
        )] <-  "VERWALTUNGS UND PRIVAT BANK AG"

data_final$sender[
  which(data_final$sender == "WELLS FARGO")] <-  "WELLS FARGO BANK"
data_final$beneficiary[
  which(data_final$beneficiary == "WELLS FARGO")] <-  "WELLS FARGO BANK"

data_final$sender[
  which(data_final$sender == "WELLS FARGO NA")] <-  "WELLS FARGO BANK NA"
data_final$beneficiary[which(data_final$beneficiary == "WELLS FARGO NA"
                             )] <-  "WELLS FARGO BANK NA"

data_final$sender[
  which(data_final$sender == "WELLSFARGO BANK NA")] <-  "WELLS FARGO BANK NA"
data_final$beneficiary[which(data_final$beneficiary == "WELLSFARGO BANK NA"
                             )] <-  "WELLS FARGO BANK NA"



# COMMAND ----------

#transorm character variables in factors

data_final$filer <- as.factor(data_final$filer)
data_final$sender <- as.factor(data_final$sender)
data_final$sender_country <- as.factor(data_final$sender_country)
data_final$sender_iso <- as.factor(data_final$sender_iso)
data_final$beneficiary <- as.factor(data_final$beneficiary)
data_final$beneficiary_country <- as.factor(data_final$beneficiary_country)
data_final$beneficiary_iso <- as.factor(data_final$beneficiary_iso)

# variable "number_of_transactions" has 111 "NA"; make it equal to 1

data_final$number_transactions[
  which(
    is.na(
      data_final$number_transactions
      )
    )
  ] <- 1


# COMMAND ----------

# there are 6 transactions with missing date, I will remove them
#LESSON to bring home: spark data frame translates empty string as NA while
# R doesn´t (it leaves the empty string)

row_to_keep <- ! is.na(data_final$begin_date)

data_final <- data_final[row_to_keep, ]


# COMMAND ----------

### variables "begin_date" and "end_date" are characters,
#make them  in date format

#1 split month day and year

split_begin <- strsplit(data_final$begin_date, split = " ")
split_end <- strsplit(data_final$end_date, split = " ")

split_begin <- matrix(unlist(split_begin), ncol = 3, byrow = T)
split_end <- matrix(unlist(split_end), ncol = 3, byrow = T)


#remove the "," symbol from the day column

split_begin[, 2] <- sub(",", "", split_begin[, 2])
split_end[, 2] <- sub(",", "", split_end[, 2])

# add a "0" to the elements of the day column if they have only one character

n_day_begin <- nchar(split_begin[, 2])
n_day_end <- nchar(split_end[, 2])

split_begin[n_day_begin == 1, 2] <- str_pad(split_begin[n_day_begin == 1, 2],
                                            side = "left", pad = "0", width = 2)

split_end[n_day_end == 1, 2] <- str_pad(split_end[n_day_end == 1, 2],
                                        side = "left", pad = "0", width = 2)


# COMMAND ----------

#change month from letter to number

split_begin[, 1][split_begin[, 1] == "Jan"] <- "01"
split_begin[, 1][split_begin[, 1] == "Feb"] <- "02"
split_begin[, 1][split_begin[, 1] == "Mar"] <- "03"
split_begin[, 1][split_begin[, 1] == "Apr"] <- "04"
split_begin[, 1][split_begin[, 1] == "May"] <- "05"
split_begin[, 1][split_begin[, 1] == "Jun"] <- "06"
split_begin[, 1][split_begin[, 1] == "Jul"] <- "07"
split_begin[, 1][split_begin[, 1] == "Aug"] <- "08"
split_begin[, 1][split_begin[, 1] == "Sep"] <- "09"
split_begin[, 1][split_begin[, 1] == "Oct"] <- "10"
split_begin[, 1][split_begin[, 1] == "Nov"] <- "11"
split_begin[, 1][split_begin[, 1] == "Dec"] <- "12"

split_end[, 1][split_end[, 1] == "Jan"] <- "01"
split_end[, 1][split_end[, 1] == "Feb"] <- "02"
split_end[, 1][split_end[, 1] == "Mar"] <- "03"
split_end[, 1][split_end[, 1] == "Apr"] <- "04"
split_end[, 1][split_end[, 1] == "May"] <- "05"
split_end[, 1][split_end[, 1] == "Jun"] <- "06"
split_end[, 1][split_end[, 1] == "Jul"] <- "07"
split_end[, 1][split_end[, 1] == "Aug"] <- "08"
split_end[, 1][split_end[, 1] == "Sep"] <- "09"
split_end[, 1][split_end[, 1] == "Oct"] <- "10"
split_end[, 1][split_end[, 1] == "Nov"] <- "11"
split_end[, 1][split_end[, 1] == "Dec"] <- "12"

# COMMAND ----------

# concatenate dates

string_begin <- apply(split_begin, 1, function(x)(paste(x, collapse = "-")))

string_end <- apply(split_end, 1, function(x)(paste(x, collapse = "-")))

#replace begin_date with string_begin and date_end with string_end in data_final

data_final$begin_date <- string_begin

data_final$end_date <- string_end

# transform begin_date and end_date as Date format

data_final$begin_date <- as.Date(data_final$begin_date, "%m-%d-%Y")

data_final$end_date <- as.Date(data_final$end_date, "%m-%d-%Y")

# COMMAND ----------

# store the final dataset

write.df(as.DataFrame(data_final),
         source = "delta",
         path = "/mnt/public/clean/fincen")
