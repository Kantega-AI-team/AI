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


# keep only usegul features
data_final <- subset(data_final, select = c(id, filer_org_name, begin_date,
                                            end_date, originator_bank,
                                            originator_bank_country,
                                            originator_iso, beneficiary_bank,
                                            beneficiary_bank_country,
                                            beneficiary_iso,
                                            number_transactions,
                                            amount_transactions))


# COMMAND ----------

# modify the name of the columns

colnames(data_final) <- c("id", "filer", "begin_date", "end_date", "sender",
                          "sender_country", "sender_iso", "beneficiary",
                          "beneficiary_country", "beneficiary_iso",
                          "number_transactions", "amount_transactions")

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
