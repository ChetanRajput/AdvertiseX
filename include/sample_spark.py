from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("My Spark App").getOrCreate()
home_dir = "/home/crajpur/airflow_spark_project/"
# Sample DataFrame creation
df = spark.read.csv(f'{home_dir}/data/input/sample.csv',header=True)
df.show()
df1 = df.where("name == 'swara'")
print("### post processing ###")
df1.show()

df1.write.csv(f'{home_dir}/data/output/family',mode='overwrite')

spark.stop()

