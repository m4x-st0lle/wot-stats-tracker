from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
from dotenv import load_dotenv
from pathlib import Path
import time
import os

load_dotenv(Path(__file__).parent.parent.parent / ".env") #bezieht sich auf den Pfad im Container

spark = (SparkSession.builder 
    .appName("WotDataTransformation") 
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

env = os.getenv("DATA_SOURCE")
account_name = os.getenv("AZURE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_BLOB_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

if env == "LOKAL":
    print("Modus: LOKAL - Lade CSV Daten")
    df = spark.read.csv("/opt/spark/work-dir/raw_data", header=True)
else:
    print(f"Modus: CLOUD - Verbinde mit Azure Account: {account_name}")
    spark.conf.set(
    f"fs.azure.account.key.{account_name}.blob.core.windows.net", 
    account_key
    )
    # Der Pfad nutzt das "wasbs" Protokoll (Windows Azure Storage Blob Secure)
    azure_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/wot_data"
    df = spark.read.parquet(azure_path)

window = Window.partitionBy("account_id").orderBy("winrate")
df = (
    df.filter(f.col("battles")>0)
    .withColumn("winrate", f.round(f.col("wins")/f.col("battles")*100))
    .withColumn("tank_rank_per_player", f.rank().over(Window.partitionBy("account_id").orderBy(f.desc("winrate"))))
    )

df.show()