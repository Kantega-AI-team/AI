# Databricks notebook source
import json

from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType

# COMMAND ----------

identity_schema = """{
  "fields": [
    {
      "metadata": {},
      "name": "TransactionID",
      "nullable": true,
      "type": "integer"
    },
    {
      "metadata": {},
      "name": "id_01",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_02",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_03",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_04",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_05",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_06",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_07",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_08",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_09",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_10",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_11",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_12",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_13",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_14",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_15",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_16",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_17",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_18",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_19",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_20",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_21",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_22",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_23",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_24",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_25",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_26",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_27",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_28",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_29",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_30",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_31",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_32",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "id_33",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_34",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_35",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_36",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_37",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "id_38",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "DeviceType",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "DeviceInfo",
      "nullable": true,
      "type": "string"
    }
  ],
  "type": "struct"
}"""


# COMMAND ----------

transaction_schema = """{
  "fields": [
    {
      "metadata": {},
      "name": "TransactionID",
      "nullable": true,
      "type": "double"
    },
    { "metadata": {}, "name": "isFraud", "nullable": true, "type": "double" },
    {
      "metadata": {},
      "name": "TransactionDT",
      "nullable": true,
      "type": "double"
    },
    {
      "metadata": {},
      "name": "TransactionAmt",
      "nullable": true,
      "type": "double"
    },
    { "metadata": {}, "name": "ProductCD", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card1", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card2", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card3", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card4", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card5", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "card6", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "addr1", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "addr2", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "dist1", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "dist2", "nullable": true, "type": "double" },
    {
      "metadata": {},
      "name": "P_emaildomain",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "R_emaildomain",
      "nullable": true,
      "type": "string"
    },
    { "metadata": {}, "name": "C1", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C2", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C3", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C4", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C5", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C6", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C7", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C8", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C9", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C10", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C11", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C12", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C13", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "C14", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D1", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D2", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D3", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D4", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D5", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D6", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D7", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D8", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D9", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D10", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D11", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D12", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D13", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D14", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "D15", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "M1", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M2", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M3", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M4", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M5", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M6", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M7", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M8", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "M9", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "V1", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V2", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V3", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V4", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V5", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V6", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V7", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V8", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V9", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V10", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V11", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V12", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V13", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V14", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V15", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V16", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V17", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V18", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V19", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V20", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V21", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V22", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V23", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V24", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V25", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V26", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V27", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V28", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V29", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V30", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V31", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V32", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V33", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V34", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V35", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V36", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V37", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V38", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V39", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V40", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V41", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V42", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V43", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V44", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V45", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V46", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V47", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V48", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V49", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V50", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V51", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V52", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V53", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V54", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V55", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V56", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V57", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V58", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V59", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V60", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V61", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V62", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V63", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V64", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V65", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V66", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V67", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V68", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V69", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V70", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V71", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V72", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V73", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V74", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V75", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V76", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V77", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V78", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V79", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V80", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V81", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V82", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V83", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V84", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V85", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V86", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V87", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V88", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V89", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V90", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V91", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V92", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V93", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V94", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V95", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V96", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V97", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V98", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V99", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V100", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V101", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V102", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V103", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V104", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V105", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V106", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V107", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V108", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V109", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V110", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V111", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V112", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V113", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V114", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V115", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V116", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V117", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V118", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V119", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V120", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V121", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V122", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V123", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V124", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V125", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V126", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V127", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V128", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V129", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V130", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V131", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V132", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V133", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V134", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V135", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V136", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V137", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V138", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V139", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V140", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V141", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V142", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V143", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V144", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V145", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V146", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V147", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V148", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V149", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V150", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V151", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V152", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V153", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V154", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V155", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V156", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V157", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V158", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V159", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V160", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V161", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V162", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V163", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V164", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V165", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V166", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V167", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V168", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V169", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V170", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V171", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V172", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V173", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V174", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V175", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V176", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V177", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V178", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V179", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V180", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V181", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V182", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V183", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V184", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V185", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V186", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V187", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V188", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V189", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V190", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V191", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V192", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V193", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V194", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V195", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V196", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V197", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V198", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V199", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V200", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V201", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V202", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V203", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V204", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V205", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V206", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V207", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V208", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V209", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V210", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V211", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V212", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V213", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V214", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V215", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V216", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V217", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V218", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V219", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V220", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V221", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V222", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V223", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V224", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V225", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V226", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V227", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V228", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V229", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V230", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V231", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V232", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V233", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V234", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V235", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V236", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V237", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V238", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V239", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V240", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V241", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V242", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V243", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V244", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V245", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V246", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V247", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V248", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V249", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V250", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V251", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V252", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V253", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V254", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V255", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V256", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V257", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V258", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V259", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V260", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V261", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V262", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V263", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V264", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V265", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V266", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V267", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V268", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V269", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V270", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V271", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V272", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V273", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V274", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V275", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V276", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V277", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V278", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V279", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V280", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V281", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V282", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V283", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V284", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V285", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V286", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V287", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V288", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V289", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V290", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V291", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V292", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V293", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V294", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V295", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V296", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V297", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V298", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V299", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V300", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V301", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V302", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V303", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V304", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V305", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V306", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V307", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V308", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V309", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V310", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V311", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V312", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V313", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V314", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V315", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V316", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V317", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V318", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V319", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V320", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V321", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V322", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V323", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V324", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V325", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V326", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V327", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V328", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V329", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V330", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V331", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V332", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V333", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V334", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V335", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V336", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V337", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V338", "nullable": true, "type": "double" },
    { "metadata": {}, "name": "V339", "nullable": true, "type": "double" }
  ],
  "type": "struct"
}"""

# COMMAND ----------

# path for identity
identity_raw_path = "/mnt/public/raw/ieee_fraud/identity*.csv"
identity_bronze_path = "/mnt/public/bronze/ieee_fraud/identity"
identity_checkpoints_path = "/mnt/public/bronze/ieee_fraud/checkpoints/identity"

# path for transaction
transaction_raw_path = "/mnt/public/raw/ieee_fraud/train_transaction*.csv"
transaction_bronze_path = "/mnt/public/bronze/ieee_fraud/transaction"
transaction_checkpoints_path = "/mnt/public/bronze/ieee_fraud/checkpoints/transaction"

# COMMAND ----------

spark.readStream.format("csv").schema(
    StructType.fromJson(json.loads(identity_schema))
).option("header", True).load(identity_raw_path).writeStream.format("delta").trigger(
    once=True
).option(
    "checkpointLocation", identity_checkpoints_path
).start(
    identity_bronze_path
)


# COMMAND ----------

spark.readStream.format("csv").schema(
    StructType.fromJson(json.loads(transaction_schema))
).option("header", True).load(transaction_raw_path).writeStream.format("delta").trigger(
    once=True
).option(
    "checkpointLocation", transaction_checkpoints_path
).start(
    transaction_bronze_path
)
