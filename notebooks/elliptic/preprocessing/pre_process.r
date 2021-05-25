# Databricks notebook source
# load required packages

library(SparkR)
library(dplyr)
library(tibble)
library(truncnorm)

# COMMAND ----------

# load original dataset with features of each transaction - note there is NOT
# header
# read as spark data frame
feat_total_db <- read.df(source = "csv",
                         path = "/mnt/public/raw/elliptic/features.csv",
                         header = "false", inferSchema = "true")


# COMMAND ----------

# load original dataset with outcome - note there is header
# read as spark data frame
y_total_db <- read.df(source = "csv",
                      path = "/mnt/public/raw/elliptic/outcome.csv",
                      header = "true", inferSchema = "true")



# COMMAND ----------

#convert spark data frame to R data.frame

feat_total <- as.data.frame(feat_total_db)

y_total <- as.data.frame(y_total_db)


# COMMAND ----------

#add column names to "feat_total"

colnames(feat_total)[1] <- "txId" # set name for ID column
colnames(feat_total)[2] <- "time" # set name for time step feature

# set name for all the features
colnames(feat_total)[3:ncol(feat_total)] <- paste0("V", 3:ncol(feat_total))

# COMMAND ----------

# select only the labeled transactions

y <- y_total[y_total$class != "unknown", ]

# new dataset with outcome and features for labelled observations only
new_data <- left_join(y, feat_total, by = c("txId" = "txId"))

# licit transactions are labelled as "0", illicit as "1"

new_data$class[new_data$class == 2] <- 0

# COMMAND ----------

## simulate cost of each transaction

# simplistic scenario
set.seed(1)
avg_ssb_minute_cost <- 12.5
avg_duration_simple <- 5 # duration for labelling simplistic scenario
simple_kr <- rtruncnorm(n = nrow(new_data),
                        a = 0,
                        b = Inf,
                        mean = avg_duration_simple,
                        sd = sqrt(1)) * avg_ssb_minute_cost

# realistic scenario
set.seed(2)
avg_duration_real <- 10 # duration for labelling realistic scenario
real_kr <- rtruncnorm(n = nrow(new_data),
                      a = 0,
                      b = Inf,
                      mean = avg_duration_real,
                      sd = sqrt(1)) * avg_ssb_minute_cost

real_kr[new_data$class == 1] <- rtruncnorm(n = sum(new_data$class == 1),
                                           a = 0,
                                           b = Inf,
                                           mean = avg_duration_real,
                                           sd = sqrt(1.5)) * avg_ssb_minute_cost

new_data <- new_data %>% add_column(simple_kr = simple_kr, real_kr = real_kr)


# COMMAND ----------


write.df(as.DataFrame(new_data),
         source = "delta",
         path = "/mnt/public/clean/elliptic")