from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to PySpark") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

# MySQL database connection properties
mysql_url = "jdbc:mysql://localhost:3306/prjdb"
table_name = "customers"
user = "root"
password = "root"

# Read data from MySQL table
df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Print schema of the DataFrame
df.printSchema()

# Show the data
df.show()

# Stop the Spark session
spark.stop()
