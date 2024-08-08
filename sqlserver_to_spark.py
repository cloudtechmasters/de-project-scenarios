from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SQL Server to PySpark") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8") \
    .getOrCreate()

# SQL Server database connection properties
sqlserver_url = "jdbc:sqlserver://localhost:1433;databaseName=your_database_name"
table_name = "your_table_name"
user = "your_username"
password = "your_password"

# Read data from SQL Server table
df = spark.read \
    .format("jdbc") \
    .option("url", sqlserver_url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

# Print schema of the DataFrame
df.printSchema()

# Show the data
df.show()

# Stop the Spark session
spark.stop()

