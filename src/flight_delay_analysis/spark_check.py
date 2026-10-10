from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("flight-delay-analysis")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [("spark-startup", True)]
columns = ["check_name", "passed"]

check_df = spark.createDataFrame(data, columns)

check_df.show()

spark.stop()