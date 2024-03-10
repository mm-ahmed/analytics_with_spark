from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("PySpark Test").getOrCreate()

master_bank_transaction_data_df = spark.read.csv("/home/mm-ahmed/Development/analytics_with_spark/data/100 BT Records.csv", header = True)

# show all columns
master_bank_transaction_data_df.show(5)

# show specific columns
master_bank_transaction_data_df.select(master_bank_transaction_data_df["Date"], master_bank_transaction_data_df["Description"]).show(5)

# replacing ',' from columns 'Deposits', 'Withdrawls, 'Balance'.
bank_transaction_data_df = master_bank_transaction_data_df.withColumn("Deposits",regexp_replace("Deposits", ",", ""))
bank_transaction_data_df = bank_transaction_data_df.withColumn("Withdrawls",regexp_replace("Withdrawls", ",", ""))
bank_transaction_data_df = bank_transaction_data_df.withColumn("Balance", regexp_replace("Balance", ",", ""))

# convert 'Deposits' column to 'Double' data-type.
transactions_less_than_50000 = bank_transaction_data_df.withColumn("Deposits",bank_transaction_data_df.Deposits.cast(DoubleType()))

# show transactions with 'Deposits', 'Withdrawls' and 'Balance' less than or equals to 50000.
transactions_less_than_50000.filter( \
                                (transactions_less_than_50000.Deposits <= 50000.0) \
                                & (transactions_less_than_50000.Withdrawls <= 50000.0) \
                                & (transactions_less_than_50000.Balance <= 50000.0) \
                            ) \
                            .show(5)

# create an empty RDD dataframe, and add a column with type.
concatenated_transactions = spark.sparkContext.emptyRDD()
schema = StructType([StructField("Date_Description_Deposits_Withdrawls_Balance", StringType())])

# show the split by comma using 'map' over 'lambda'.
concatenated_transactions = bank_transaction_data_df.withColumn(
                                ("Date_Description_Deposits_Withdrawls_Balance"), \
                                concat( \
                                    "Date", \
                                    lit(", "), \
                                    "Description", \
                                    lit(", "), \
                                    "Deposits", \
                                    lit(", "), \
                                    "Balance", \
                                )
                            ).select("Date_Description_Deposits_Withdrawls_Balance")

bank_transaction_data_df.show(5)
concatenated_transactions.show(5)

spark.stop()
