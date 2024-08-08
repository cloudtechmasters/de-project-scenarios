from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL to PySpark") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# PostgreSQL database connection properties
postgres_url = "jdbc:postgresql://your_postgres_host:5432/your_database_name"
table_name = "your_table_name"
user = "your_username"
password = "your_password"

# Read data from PostgreSQL table
df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Print schema of the DataFrame
df.printSchema()

# Show the data
df.show()

# Stop the Spark session
spark.stop()

