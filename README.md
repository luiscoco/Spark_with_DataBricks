# Udemy Apache Spark 3. Big Data Essentials in Scala Rock the JVM

https://github.com/rockthejvm/spark-essentials

# 1. Welcome

## 1.1. DataBricks Community

https://community.cloud.databricks.com/

## 1.2. Scala Recap

## 1.3. Spark First Principles



# 2. Spark Structured and API: DataFrames

## 2.1. DataFrames Basics

In Scala Spark, a DataFrame is a distributed collection of data organized into named columns.

It is similar to a table in a relational database or a data frame in R/Python. 

Spark DataFrames provide a higher-level API compared to RDDs, making it easier to perform data manipulation tasks.

```scala
%scala
// Import SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create a SparkSession
val spark = SparkSession.builder
  .appName("DataFrame Basics")
  .getOrCreate()

// Create a simple DataFrame with some data
val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35))
val columns = Seq("Name", "Age")

import spark.implicits._
val df = data.toDF(columns: _*)

// Show the DataFrame
df.show()

// Select a specific column
val nameColumn = df("Name")

// Instead of nameColumn.show(), use df.select("Name").show()
df.select("Name").show()

// Filter the DataFrame based on a condition
val filteredDF = df.filter($"Age" > 30)
filteredDF.show()

// Perform some aggregation (e.g., calculate the average age)
val avgAge = df.agg(avg($"Age").as("AverageAge"))
avgAge.show()

// Join two DataFrames
val otherData = Seq(("Alice", "Engineer"), ("Bob", "Doctor"))
val otherColumns = Seq("Name", "Occupation")
val otherDF = otherData.toDF(otherColumns: _*)

val joinedDF = df.join(otherDF, "Name")
joinedDF.show()

// Stop the SparkSession
spark.stop()
```

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/a68d306e-e5a2-48e7-8596-9c89c8717df9)

Let me explain what's happening in the code:

a) **Creating SparkSession:** The entry point for Spark functionality.

b) **Creating a DataFrame:** Using a sequence of data and column names.

c) **Showing the DataFrame:** Displaying the content of the DataFrame.

d) **Selecting a Column:** Accessing a specific column.

e) **Filtering Data:** Applying a filter condition.

f) **Aggregation:** Calculating the average age.

g) **Joining DataFrames:** Combining two DataFrames based on a common column.

h) **Stopping SparkSession:** Ending the SparkSession.

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/2b3046e7-5861-4548-98bb-a756be92cbe8)

## 2.2. DataFrames Basics. Exercises

Here are a few more examples of common operations you might perform with DataFrames in Scala Spark:

### Reading Data from a File with Scala Spark DataBricks:

First we login in DataBricks Community edition

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/0c076041-491a-4e76-affb-9fef6ae7f33d)

Second we create a new Notebook

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/36af7962-b3fb-4d73-aa50-2f7ea3188843)

Third step we enter the scala code and we select in the 

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/d3dac90b-f981-406b-9af1-11af96e3b47f)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/b3d24d39-6ea6-4321-bf95-88e7041982e6)

Four step we create a cluster and attach to it

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/ef6dca8f-ff56-46c7-868a-26c18162c3bc)

Fifth step we input the scala source code

```scala
%scala
// Read a CSV file into a DataFrame
val csvPath = "/FileStore/tables/fileCSV.csv"  // Specify the correct DBFS path
val csvDF = spark.read.csv(csvPath)

// Show the content of the DataFrame
csvDF.show()
```

Sixth step, we create a CSV file in my local laptop

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/4967159e-dc55-4323-a5e9-c0c4dfc1732b)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/2f5d5065-b234-4c16-aa5f-5f950b244f72)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/be1f8d7b-61f1-43bf-980d-a3ff181fe864)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/70e019ba-cf1a-4d59-8617-c600e35a7a13)

Seven step, click on workspace and create a new folder

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/a6324572-a9a8-4092-9d14-c5f0e4f74c6c)

Eight step, click on the folder and then click on the Data button

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/e246dfdb-2679-482d-9b52-85fa4dd860e9)

Nine step, press in the "Create Table" button

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/9eb6dd4b-4f3a-4ef7-a994-a35c240e1440)

Ten step, we drag and drop the CSV file 

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/d47f4769-f003-4d9a-80af-d4a6ca4c4d0c)

Eleven step, we copy the uploaded file path

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/b6ba087f-8d92-4846-9148-82899bd21cea)

Then we click on Workspace and then we double click on the Notebook we would like to open

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/385377f5-1ea3-41ab-90f3-c136de141aac)

Finally we press on the Run button

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/1616db22-74a4-4418-ba56-09ee4897968c)

Then we can see the output result

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/7798c473-ed2a-405f-b0fd-46bd05ccb7e1)

See other code samples: 

```scala
// Read a CSV file into a DataFrame
val csvPath = "/path/to/your/file.csv"
val csvDF = spark.read.csv(csvPath)

// Read a Parquet file into a DataFrame
val parquetPath = "/path/to/your/file.parquet"
val parquetDF = spark.read.parquet(parquetPath)
```

### Writing Data to a File:

```scala
// Write a DataFrame to a CSV file
val outputCsvPath = "/path/to/your/output.csv"
csvDF.write.csv(outputCsvPath)

// Write a DataFrame to a Parquet file
val outputParquetPath = "/path/to/your/output.parquet"
parquetDF.write.parquet(outputParquetPath)
```

### Grouping and Aggregation:

```scala
// Group by a column and calculate the average age
val groupedDF = df.groupBy("Occupation").agg(avg("Age").as("AverageAge"))
groupedDF.show()
```

### Adding a New Column:

```scala
// Add a new column based on a condition
val newDF = df.withColumn("Status", when($"Age" > 30, "Senior").otherwise("Junior"))
newDF.show()
```

### Filtering with SQL Expression:

```scala
// Filter the DataFrame using SQL expression
val filteredSQLDF = df.filter("Age > 30")
filteredSQLDF.show()
```

### Running SQL Queries:

```scala
Copy code
// Create a temporary SQL table
df.createOrReplaceTempView("people")

// Run a SQL query on the DataFrame
val sqlResult = spark.sql("SELECT * FROM people WHERE Age > 30")
sqlResult.show()
```

### Handling Missing Values:

```scala
// Drop rows with any missing values
val dfWithoutNull = df.na.drop()

// Fill missing values with a specific value
val dfFilled = df.na.fill(0)
```

### Exploding Arrays:

```scala
// Explode a column of arrays into separate rows
val arrayDF = Seq((1, Seq("apple", "orange")), (2, Seq("banana", "grape"))).toDF("id", "fruits")
val explodedDF = arrayDF.select($"id", explode($"fruits").as("fruit"))
explodedDF.show()
```

## 2.3. How DataFrames Work

In Apache Spark, a DataFrame is a distributed collection of data organized into named columns.

It is conceptually similar to a table in a relational database, or a data frame in R/Python, but with optimizations for distributed computing.

Here's a brief overview of how DataFrames work in Scala with Spark:

### Creation:

You can create a DataFrame from various sources, such as Hive tables, external databases, existing RDDs (Resilient Distributed Datasets), or even from local collections.

```scala
// Import necessary libraries
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

// Create a Spark session
val spark = SparkSession.builder.appName("example").getOrCreate()

// Define the schema
val schema = StructType(
  Array(
    StructField("Name", StringType, true),
    StructField("Age", IntegerType, true)
  )
)

// Provide sample data
val data = Seq(
  Row("John", 28),
  Row("Alice", 22),
  Row("Bob", 25)
)

// Create DataFrame
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

// Show the DataFrame
df.show()
```

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/b79379d6-23c2-4d7d-ab12-9f4479251c7c)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/13d7601e-53e8-4998-a1a6-139953169ea3)

### Transformation:

DataFrames support a wide range of operations, such as filtering, selecting, grouping, and aggregating data. 

These operations are performed in a similar manner to SQL queries.

Here's an example that demonstrates filtering, selecting, grouping, and aggregating operations on the DataFrame created with the sample data:

```scala
// Import necessary libraries
import org.apache.spark.sql.functions._

// Filter the DataFrame to select people with age greater than 25
val filteredDF = df.filter("Age > 25")

// Select only the "Name" column
val selectedDF = df.select("Name")

// Group the DataFrame by age and count the occurrences
val groupedDF = df.groupBy("Age").count()

// Aggregate the DataFrame to find the average age
val avgAgeDF = df.agg(avg("Age"))

// Show the results
println("Filtered DataFrame:")
filteredDF.show()

println("Selected DataFrame:")
selectedDF.show()

println("Grouped DataFrame:")
groupedDF.show()

println("Average Age:")
avgAgeDF.show()
```

This code snippet demonstrates:

Filtering: Selecting individuals with an age greater than 25.

Selecting: Choosing only the "Name" column.

Grouping: Grouping the DataFrame by the "Age" column.

Aggregating: Calculating the average age of all individuals.

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/b516c2fe-72c4-48cd-a0ef-cbf9816708c0)

This is the output:

```
Filtered DataFrame:
+----+---+
|Name|Age|
+----+---+
|John| 28|
+----+---+

Selected DataFrame:
+-----+
| Name|
+-----+
| John|
|Alice|
|  Bob|
+-----+

Grouped DataFrame:
+---+-----+
|Age|count|
+---+-----+
| 28|    1|
| 22|    1|
| 25|    1|
+---+-----+

Average Age:
+--------+
|avg(Age)|
+--------+
|    25.0|
+--------+
```

### Action:

Actions are operations that trigger the execution of the computation plan. 

Examples include show() to display the data, count() to count the number of rows, and collect() to retrieve all data to the driver program.

```scala
// Display the first 10 rows
selectedDF.show(10)

// Count the number of rows
val rowCount = selectedDF.count()
```

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/3f85c5e2-77c0-453e-ad76-bbf48fea155a)

### Integration with Spark SQL:

DataFrames seamlessly integrate with Spark SQL, allowing you to run SQL queries on your DataFrames.

```scala
// Registering the DataFrame as a temporary table
selectedDF.createOrReplaceTempView("myTable")

// Running SQL queries on the DataFrame with TRIM
val result = spark.sql("SELECT Name FROM myTable WHERE TRIM(Name) = 'Alice' LIMIT 1")
// Collect the result and print the value
val aliceValue = result.collect()(0)(0).toString
println(aliceValue)
```

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/d7d312b0-e6b5-4736-9a33-8db27a77daa2)

### Optimizations:

Spark optimizes the execution plan of DataFrames using a query optimizer called Catalyst. 

This helps in improving performance by optimizing the physical execution plan of the operations.

Here, spark is an instance of SparkSession, which is the entry point to programming Spark with the DataFrame and SQL API. 

It provides a way to configure Spark application settings and access Spark functionality.

Remember that Spark is designed for distributed computing, and DataFrames are processed in parallel across a cluster of machines.

Spark provides various optimizations to improve the performance of DataFrame operations. Let's explore a few optimization techniques:

### Predicate Pushdown:

Spark's Catalyst optimizer can push down filters closer to the data source. 

This means that if you filter a DataFrame using a condition, Spark may optimize the physical execution plan by applying the filter as close to the data source as possible, reducing the amount of data that needs to be processed.

```scala
val result = spark.sql("SELECT * FROM myTable WHERE age > 21")
```

In this example, if the "age" column is part of the data source, Spark may push down the filter directly to the data source.

### Projection Pushdown:

Similar to predicate pushdown, Spark can optimize queries by pushing down projections. 

If your query only needs a subset of columns, Spark may optimize the execution plan by selecting only those columns at an earlier stage, reducing data movement.

```scala
val result = spark.sql("SELECT name, age FROM myTable")
```

In this case, if the data source supports projection pushdown, Spark may optimize the query by selecting only the "name" and "age" columns at the source.

### Broadcast Joins:

Spark can optimize join operations by broadcasting smaller DataFrames to all nodes in the cluster, avoiding shuffling of large amounts of data.

```scala
val result = df1.join(broadcast(df2), "id")
```

The broadcast function hints Spark to use a broadcast join for the smaller DataFrame (df2 in this case).

### Caching:

You can explicitly cache DataFrames or tables in memory to avoid recomputing them. This can be useful when you have iterative algorithms or when a DataFrame is reused multiple times.

```scala
myTable.cache()
```

This caches the DataFrame myTable in memory, and subsequent operations on it may benefit from the cached data.

### Partitioning:

Ensuring that your DataFrames are properly partitioned can significantly improve performance, especially when performing operations that involve shuffling.

```scala
val partitionedDF = myTable.repartition(col("someColumn"))
```

This repartitions the DataFrame based on the values in "someColumn," which can optimize certain operations.

## 2.4. Data Sources

Sure, when it comes to working with Scala and Spark in Databricks, you might want to leverage various data sources.

### CSV:

```scala
%scala
// Read a CSV file into a DataFrame
val csvPath = "/FileStore/tables/fileCSV.csv"  // Specify the correct DBFS path
val csvDF = spark.read.csv(csvPath)

// Show the content of the DataFrame
csvDF.show()
```

### JSON:

```scala
%scala
// Read a json file into a DataFrame
val jsonPath = "/FileStore/tables/cars-1.json"  // Specify the correct DBFS path
val jsonDF = spark.read.json(jsonPath)
// Show the content of the DataFrame
jsonDF.show()
```

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/0d0c6c44-8feb-4e7e-b9d1-bed957083aca)

### Parquet:

```scala
val df = spark.read.parquet("/path/to/parquet/file")
```

### Avro:

```scala
val df = spark.read.format("avro").load("/path/to/avro/file")
```

### Delta Lake:

```scala
Copy code
val df = spark.read.format("delta").load("/path/to/delta/lake")
```

### ORC:

```scala
val df = spark.read.format("orc").load("/path/to/orc/file")
```

### JDBC:

```scala
val jdbcUrl = "jdbc:mysql://hostname:port/dbname"
val df = spark.read.format("jdbc").option("url", jdbcUrl).option("dbtable", "tablename").load()
```

### Cassandra:

```scala
val df = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "keyspace").option("table", "table").load()
```

Remember to replace the file paths and connection details with your specific information. 

These examples assume that you have a SparkSession named spark already created.

## 2.5. Data Sources. Exercises

## 2.6. DataFrames Columns and Expressions

In Apache Spark with Scala, DataFrames are a fundamental abstraction representing a distributed collection of data organized into named columns. 

Spark DataFrames can be created from various data sources, and once created, you can perform operations on them using transformations and actions.

### Creating a DataFrame:

```scala
// Import necessary libraries
import org.apache.spark.sql.{SparkSession, DataFrame}

// Create a Spark session
val spark = SparkSession.builder.appName("example").getOrCreate()

// Sample data
val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 22))

// Define the schema for the DataFrame
val schema = List("Name", "Age")

// Create a DataFrame
val df = spark.createDataFrame(data).toDF(schema: _*)
```

### Showing the DataFrame:

```scala
df.show()
```

### Selecting Columns:

```scala
// Selecting a single column
val nameColumn = df("Name")

// Selecting multiple columns
val selectedColumns = df.select("Name", "Age")
```

### Adding a New Column:

```scala
Copy code
// Adding a new column
val updatedDF = df.withColumn("AgeAfter5Years", df("Age") + 5)
```

### Filtering Rows:

```scala
// Filtering based on a condition
val filteredDF = df.filter(df("Age") > 25)
```

### Grouping and Aggregating:

```scala
// Grouping by a column and calculating the average
val groupedDF = df.groupBy("Name").agg(avg("Age"))
```

### Joining DataFrames:

```scala
// Create another DataFrame
val otherData = Seq(("Alice", "Engineer"), ("Bob", "Doctor"))
val otherDF = spark.createDataFrame(otherData).toDF("Name", "Occupation")

// Joining DataFrames
val joinedDF = df.join(otherDF, "Name")
```

These are just some basic operations. 

Spark supports a rich set of transformations and actions that you can perform on DataFrames. 

DataFrames provide a high-level API that abstracts away many of the complexities of distributed processing.

## 2.7. DataFrames Columns and Expressions. Exercises

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/b4e90255-078f-4453-833b-e05e49d1c1bb)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/c508bf8b-78fa-4050-8862-2ced4ad74260)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/34e61242-2c85-476f-bd31-c7059445b78f)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/df9a622d-0d61-4a9f-beed-cbc314219589)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/576a58d2-af21-4227-bcb9-843042a8a842)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/dd7cc6f1-4984-4b57-af8d-a013e6713ea4)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/bfa45f5e-531b-4a2e-a3ed-26079e777ef9)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/a6854971-119f-4dcc-aeb8-0fa2888f79f6)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/073a3542-dcb4-4654-be2f-9c4396ebc14e)

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/055a98a6-0217-444e-816b-a98f0cbb80a5)

## 2.8. DataFrame Aggregations

I'd be happy to help you with DataFrame aggregations in Scala using Apache Spark in Databricks! 

DataFrame aggregations involve grouping data based on one or more columns and then performing some aggregate functions on the grouped data.

Let's say you have a DataFrame called df with columns name, age, and salary.

```scala
// Import necessary Spark libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Assuming you already have a DataFrame called df
// If not, you can read data into a DataFrame using spark.read

// Adding a 'Salary' column with sample values
val dfWithSalary = df.withColumn("Salary", lit(50000)) // You can replace 50000 with your desired salary value

// Display the DataFrame with the added 'Salary' column
dfWithSalary.show()

// 1. Group by 'Name' and calculate the average salary
val avgSalaryDF = dfWithSalary.groupBy("Name").agg(avg("Salary").as("average_salary"))

// Display the result
avgSalaryDF.show()

// 2. Group by 'Age' and get the maximum salary
val maxSalaryDF = dfWithSalary.groupBy("Age").agg(max("Salary").as("max_salary"))

// Display the result
maxSalaryDF.show()

// 3. Use Window functions to calculate the rank based on salary
val windowSpec = Window.orderBy(desc("Salary"))
val rankDF = dfWithSalary.withColumn("rank", rank().over(windowSpec))

// Display the result
rankDF.show()

// 4. Group by 'Name' and get the count of records for each name
val countDF = dfWithSalary.groupBy("Name").agg(count("*").as("record_count"))

// Display the result
countDF.show()
```

In this example:

groupBy is used to specify the columns for grouping.

agg is used to perform aggregate functions like avg, max, count, etc.

Window functions (like rank in this case) are used for operations that involve sorting and ranking within partitions.

Remember to adjust column names and aggregation functions based on your actual DataFrame structure and requirements.

## 2.8. DataFrame Aggregations. Exercises

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/1c6a4c04-ea77-48df-94d7-6606f8cf14de)

```
(8) Spark Jobs
dfWithSalary:org.apache.spark.sql.DataFrame = [Name: string, Age: integer ... 1 more field]
avgSalaryDF:org.apache.spark.sql.DataFrame = [Name: string, average_salary: double]
maxSalaryDF:org.apache.spark.sql.DataFrame = [Age: integer, max_salary: integer]
rankDF:org.apache.spark.sql.DataFrame = [Name: string, Age: integer ... 2 more fields]
countDF:org.apache.spark.sql.DataFrame = [Name: string, record_count: long]
+-------+---+------+
|   Name|Age|Salary|
+-------+---+------+
|  Alice| 25| 50000|
|    Bob| 30| 50000|
|Charlie| 22| 50000|
+-------+---+------+

+-------+--------------+
|   Name|average_salary|
+-------+--------------+
|  Alice|       50000.0|
|    Bob|       50000.0|
|Charlie|       50000.0|
+-------+--------------+

+---+----------+
|Age|max_salary|
+---+----------+
| 25|     50000|
| 30|     50000|
| 22|     50000|
+---+----------+

+-------+---+------+----+
|   Name|Age|Salary|rank|
+-------+---+------+----+
|  Alice| 25| 50000|   1|
|    Bob| 30| 50000|   1|
|Charlie| 22| 50000|   1|
+-------+---+------+----+

+-------+------------+
|   Name|record_count|
+-------+------------+
|  Alice|           1|
|    Bob|           1|
|Charlie|           1|
+-------+------------+

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
dfWithSalary: org.apache.spark.sql.DataFrame = [Name: string, Age: int ... 1 more field]
avgSalaryDF: org.apache.spark.sql.DataFrame = [Name: string, average_salary: double]
maxSalaryDF: org.apache.spark.sql.DataFrame = [Age: int, max_salary: int]
windowSpec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.WindowSpec@6841f603
rankDF: org.apache.spark.sql.DataFrame = [Name: string, Age: int ... 2 more fields]
countDF: org.apache.spark.sql.DataFrame = [Name: string, record_count: bigint]
```

Ýou can also assign different salaries values to each row:

```scala
// Adding a 'Salary' column with different values based on age
val dfWithDifferentSalaries = df.withColumn(
  "Salary",
  when(col("Age") < 25, lit(45000))
    .when(col("Age") >= 25 && col("Age") < 30, lit(55000))
    .otherwise(lit(60000)) // Default salary for other cases
)
```

## 2.9. DataFrame Joins

In Scala Spark with DataBricks, DataFrame joins are a common operation when you want to combine two DataFrames based on a common column. 

There are several types of joins available, such as inner join, outer join, left join, and right join.

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/1cea81a1-499a-4fae-871f-498d8823ef99)

Let's assume you have two DataFrames, df1 and df2, and you want to join them based on a common column, say "commonColumn".

### Inner Join:

An inner join returns only the rows where there is a match in both DataFrames.

```scala
val resultDF = df1.join(df2, "commonColumn")
````

### Left Join:

A left join returns all the rows from the left DataFrame (df1) and the matched rows from the right DataFrame (df2). 

If there is no match, it fills with null values.

```scala
val resultDF = df1.join(df2, Seq("commonColumn"), "left")
```

### Right Join:

A right join returns all the rows from the right DataFrame (df2) and the matched rows from the left DataFrame (df1). 

If there is no match, it fills with null values.

```scala
val resultDF = df1.join(df2, Seq("commonColumn"), "right")
```

### Outer Join (Full Outer Join):

An outer join returns all the rows when there is a match in either the left or right DataFrame. If there is no match, it fills with null values.

```scala
val resultDF = df1.join(df2, Seq("commonColumn"), "outer")
```

Here's a simple example with some random data:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("DataFrameJoinsExample").getOrCreate()

// Sample DataFrames
val data1 = Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3))
val data2 = Seq(("Alice", "Engineer"), ("Bob", "Doctor"), ("David", "Artist"))

val df1 = spark.createDataFrame(data1).toDF("Name", "Value")
val df2 = spark.createDataFrame(data2).toDF("Name", "Profession")

// Inner Join
val innerJoinDF = df1.join(df2, "Name")
innerJoinDF.show()

// Left Join
val leftJoinDF = df1.join(df2, Seq("Name"), "left")
leftJoinDF.show()

// Right Join
val rightJoinDF = df1.join(df2, Seq("Name"), "right")
rightJoinDF.show()

// Outer Join
val outerJoinDF = df1.join(df2, Seq("Name"), "outer")
outerJoinDF.show()
```

## 2.10. DataFrame Joins. Exercises

![image](https://github.com/luiscoco/Udemy_Apache_Spark_3_Big_Data_Essentials_in_Scala_Rock_the_JVM/assets/32194879/7219719a-ce9a-4ebc-8251-1795a69749d4)

```
(9) Spark Jobs
df1:org.apache.spark.sql.DataFrame = [Name: string, Value: integer]
df2:org.apache.spark.sql.DataFrame = [Name: string, Profession: string]
innerJoinDF:org.apache.spark.sql.DataFrame = [Name: string, Value: integer ... 1 more field]
leftJoinDF:org.apache.spark.sql.DataFrame = [Name: string, Value: integer ... 1 more field]
rightJoinDF:org.apache.spark.sql.DataFrame = [Name: string, Value: integer ... 1 more field]
outerJoinDF:org.apache.spark.sql.DataFrame = [Name: string, Value: integer ... 1 more field]
+-----+-----+----------+
| Name|Value|Profession|
+-----+-----+----------+
|Alice|    1|  Engineer|
|  Bob|    2|    Doctor|
+-----+-----+----------+

+-------+-----+----------+
|   Name|Value|Profession|
+-------+-----+----------+
|  Alice|    1|  Engineer|
|    Bob|    2|    Doctor|
|Charlie|    3|      null|
+-------+-----+----------+

+-----+-----+----------+
| Name|Value|Profession|
+-----+-----+----------+
|Alice|    1|  Engineer|
|  Bob|    2|    Doctor|
|David| null|    Artist|
+-----+-----+----------+

+-------+-----+----------+
|   Name|Value|Profession|
+-------+-----+----------+
|  Alice|    1|  Engineer|
|    Bob|    2|    Doctor|
|Charlie|    3|      null|
|  David| null|    Artist|
+-------+-----+----------+

import org.apache.spark.sql.SparkSession
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@427114a9
data1: Seq[(String, Int)] = List((Alice,1), (Bob,2), (Charlie,3))
data2: Seq[(String, String)] = List((Alice,Engineer), (Bob,Doctor), (David,Artist))
df1: org.apache.spark.sql.DataFrame = [Name: string, Value: int]
df2: org.apache.spark.sql.DataFrame = [Name: string, Profession: string]
innerJoinDF: org.apache.spark.sql.DataFrame = [Name: string, Value: int ... 1 more field]
leftJoinDF: org.apache.spark.sql.DataFrame = [Name: string, Value: int ... 1 more field]
rightJoinDF: org.apache.spark.sql.DataFrame = [Name: string, Value: int ... 1 more field]
outerJoinDF: org.apache.spark.sql.DataFrame = [Name: string, Value: int ... 1 more field]
```

# 3. Spark Types and Datasets

## 3.1. Working with Common Spark Data Types

Let's dive into the common Spark Data Types in Scala using DataFrames in Apache Spark with DataBricks. 

Spark DataFrames are built on top of the Spark SQL engine, and they provide a distributed collection of data organized into named columns.

Here are some common Spark Data Types and examples using Scala with DataFrames:

### 1. StringType:

Represents strings of characters. In Scala, it's represented as StringType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("name", StringType, true)))
```

### 2. IntegerType:

Represents 32-bit signed integers. In Scala, it's represented as IntegerType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("age", IntegerType, true)))
```

### 3. LongType:

Represents 64-bit signed integers. In Scala, it's represented as LongType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("salary", LongType, true)))
```

### 4. DoubleType:

Represents 64-bit double-precision floating-point numbers. In Scala, it's represented as DoubleType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("rating", DoubleType, true)))
```

### 5. BooleanType:

Represents boolean values true or false. In Scala, it's represented as BooleanType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("isStudent", BooleanType, true)))
```

### 6. ArrayType:

Represents arrays of elements. In Scala, it's represented as ArrayType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("grades", ArrayType(IntegerType), true)))
```

### 7. MapType:

Represents key-value pairs. In Scala, it's represented as MapType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("gradesMap", MapType(StringType, IntegerType), true)))
```

### 8. StructType:

Represents a structure or a record. In Scala, it's represented as StructType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true)
))
```

### 9. TimestampType:

Represents a timestamp with a time zone. In Scala, it's represented as TimestampType.

```scala
import org.apache.spark.sql.types._

val schema = StructType(Seq(StructField("eventTime", TimestampType, true)))
```

These examples demonstrate the basic usage of common Spark Data Types in Scala using DataFrames. 

You can use these types to define the schema of your DataFrames when working with Spark.

### Data Types example
Let's include examples of how to create DataFrames based on the specified schema and populate them with data.

```scala
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

// Create a Spark session
val spark = SparkSession.builder.appName("SparkExample").getOrCreate()

// Define the schema
val schema = StructType(Seq(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true),
  StructField("salary", LongType, true),
  StructField("rating", DoubleType, true),
  StructField("isStudent", BooleanType, true),
  StructField("grades", ArrayType(IntegerType), true),
  StructField("gradesMap", MapType(StringType, IntegerType), true),
  StructField("eventTime", TimestampType, true)
))

// Create a DataFrame with the specified schema
val data = Seq(
  Row("John", 25, 50000L, 4.5, true, Seq(90, 85, 92), Map("Math" -> 90, "English" -> 85), java.sql.Timestamp.valueOf("2023-10-25 12:30:00")),
  Row("Alice", 22, 60000L, 4.8, false, Seq(95, 88, 94), Map("Math" -> 95, "English" -> 88), java.sql.Timestamp.valueOf("2023-10-25 13:15:00"))
)

// Create the DataFrame
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

// Show the DataFrame
df.show()
```

In this example, we first define the schema using StructType and then create a DataFrame (df) using the createDataFrame method. 

The Row class is used to represent each row of data, and we create an RDD (Resilient Distributed Dataset) from a sequence of rows. 

The show() method is then used to display the contents of the DataFrame.

```
(3) Spark Jobs
df:org.apache.spark.sql.DataFrame = [name: string, age: integer ... 6 more fields]
+-----+---+------+------+---------+------------+--------------------+-------------------+
| name|age|salary|rating|isStudent|      grades|           gradesMap|          eventTime|
+-----+---+------+------+---------+------------+--------------------+-------------------+
| John| 25| 50000|   4.5|     true|[90, 85, 92]|{Math -> 90, Engl...|2023-10-25 12:30:00|
|Alice| 22| 60000|   4.8|    false|[95, 88, 94]|{Math -> 95, Engl...|2023-10-25 13:15:00|
+-----+---+------+------+---------+------------+--------------------+-------------------+

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@427114a9
schema: org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true),StructField(age,IntegerType,true),StructField(salary,LongType,true),StructField(rating,DoubleType,true),StructField(isStudent,BooleanType,true),StructField(grades,ArrayType(IntegerType,true),true),StructField(gradesMap,MapType(StringType,IntegerType,true),true),StructField(eventTime,TimestampType,true))
data: Seq[org.apache.spark.sql.Row] = List([John,25,50000,4.5,true,List(90, 85, 92),Map(Math -> 90, English -> 85),2023-10-25 12:30:00.0], [Alice,22,60000,4.8,false,List(95, 88, 94),Map(Math -> 95, English -> 88),2023-10-25 13:15:00.0])
df: org.apache.spark.sql.DataFrame = [name: string, age: int ... 6 more fields]
```

## 3.2. Working with Complex Spark Data Types

Spark provides support for complex data types, allowing you to work with nested and structured data in a distributed computing environment. 

The most common complex data types in Spark are: StructType, StructField, ArrayType, MapType, and nested StructTypes.

Let's start with **StructType** and **StructField**. 

These are used to define a structure for your data, similar to a table schema. Here's an example:

```scala
import org.apache.spark.sql.types._

// Define a schema with two fields: name and age
val schema = StructType(Seq(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true)
))

// Create a DataFrame with the defined schema
val data = Seq(("John", 25), ("Jane", 30), ("Doe", null))
val df = spark.createDataFrame(data).toDF("name", "age")

// Apply the schema to the DataFrame
val dfWithSchema = spark.createDataFrame(df.rdd, schema)

dfWithSchema.show()
```

Next, let's look at **ArrayType**. This is used for representing arrays or lists in your data:

```scala
// Define a schema with an array of integers
val arraySchema = StructType(Seq(
  StructField("numbers", ArrayType(IntegerType, true), true)
))

// Create a DataFrame with the defined schema
val arrayData = Seq((Seq(1, 2, 3)), (Seq(4, 5)), (null))
val arrayDF = spark.createDataFrame(arrayData).toDF("numbers")

// Apply the schema to the DataFrame
val arrayDFWithSchema = spark.createDataFrame(arrayDF.rdd, arraySchema)

arrayDFWithSchema.show()
```

Now, let's explore **MapType**, which is used to represent key-value pairs:

```scala
// Define a schema with a map of string keys and integer values
val mapSchema = StructType(Seq(
  StructField("info", MapType(StringType, IntegerType, true), true)
))

// Create a DataFrame with the defined schema
val mapData = Seq((Map("score" -> 90, "rank" -> 1)), (Map("score" -> 85)), (null))
val mapDF = spark.createDataFrame(mapData).toDF("info")

// Apply the schema to the DataFrame
val mapDFWithSchema = spark.createDataFrame(mapDF.rdd, mapSchema)

mapDFWithSchema.show()
```

These examples showcase the use of complex data types in Spark DataFrames using Scala. 

They provide a way to represent and work with structured and nested data efficiently in a distributed computing environment.

### More advanced examples using Spark's complex data types with Scala in a Databricks environment

Nested Structures with **StructType**:

```scala
import org.apache.spark.sql.types._

// Define a schema with nested structures
val nestedSchema = StructType(Seq(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true),
  StructField("address", StructType(Seq(
    StructField("city", StringType, true),
    StructField("state", StringType, true)
  )), true)
))

// Create a DataFrame with the defined schema
val nestedData = Seq(("John", 25, ("San Francisco", "CA")), ("Jane", 30, ("New York", "NY")), (null, null, null))
val nestedDF = spark.createDataFrame(nestedData).toDF("name", "age", "address")

// Apply the schema to the DataFrame
val nestedDFWithSchema = spark.createDataFrame(nestedDF.rdd, nestedSchema)

nestedDFWithSchema.show()
```

**Arrays of StructType**:

```scala
Copy code
// Define a schema with an array of structures
val arrayOfStructSchema = StructType(Seq(
  StructField("person", StringType, true),
  StructField("contacts", ArrayType(StructType(Seq(
    StructField("type", StringType, true),
    StructField("number", StringType, true)
  )), true), true)
))

// Create a DataFrame with the defined schema
val arrayOfStructData = Seq(("John", Seq(("email", "john@example.com"), ("phone", "123-456-7890"))), 
                            ("Jane", Seq(("email", "jane@example.com"))))
val arrayOfStructDF = spark.createDataFrame(arrayOfStructData).toDF("person", "contacts")

// Apply the schema to the DataFrame
val arrayOfStructDFWithSchema = spark.createDataFrame(arrayOfStructDF.rdd, arrayOfStructSchema)

arrayOfStructDFWithSchema.show()
```

**MapType** with Nested Structures:

```scala
// Define a schema with a map of string keys and nested structures as values
val mapOfStructSchema = StructType(Seq(
  StructField("attributes", MapType(StringType, StructType(Seq(
    StructField("value", StringType, true),
    StructField("unit", StringType, true)
  )), true), true)
))

// Create a DataFrame with the defined schema
val mapOfStructData = Seq((Map("height" -> ("5.8", "feet"), "weight" -> ("150", "lbs"))), 
                          (Map("height" -> ("5.5", "feet"))))
val mapOfStructDF = spark.createDataFrame(mapOfStructData).toDF("attributes")

// Apply the schema to the DataFrame
val mapOfStructDFWithSchema = spark.createDataFrame(mapOfStructDF.rdd, mapOfStructSchema)

mapOfStructDFWithSchema.show()
```

These examples demonstrate more advanced use cases of complex data types in Spark DataFrames. 

They include nested structures, arrays of structures, and MapType with nested structures as values, providing flexibility to handle diverse data scenarios in distributed computing environments.

## 3.3. Managing Nulls in Data

Dealing with null values is an essential part of data processing, and Scala Spark in Databricks provides several ways to handle them. Here are some common techniques:

### Dropping Null Values:

drop method is used to eliminate rows with null values.

```scala
val dfWithoutNulls = originalDF.na.drop()
````

This removes any row containing at least one null value.

### Filling Null Values:

fill method can be used to replace null values with specific values.

```scala
val dfFilled = originalDF.na.fill("default_value")
```

Replace nulls with a default value.

### Imputing Null Values:

Imputation involves replacing null values with some calculated values, often the mean or median of the column.

```scala
val meanValue = originalDF.select(avg("column_name")).first()(0).asInstanceOf[Double]
val dfImputed = originalDF.na.fill(meanValue, Seq("column_name"))
```

Replace nulls in a specific column with the mean value.

### Handling Nulls in Conditions:

You can use isNull or isNotNull functions for filtering based on null values.

```scala
val dfNotNull = originalDF.filter(col("column_name").isNotNull)
```

This keeps only the rows where a specific column is not null.

### Coalesce:

coalesce can be used to select the first non-null value from a set of columns.

```scala
Copy code
val dfCoalesced = originalDF.withColumn("new_column", coalesce(col("column1"), col("column2")))
```

Create a new column with the first non-null value from two existing columns.

Remember, the choice of method depends on the specific requirements of your data analysis. 

You might need to use a combination of these techniques based on the nature of your data.

## 3.3. More advanced topics related to managing nulls in Scala Spark on Databricks

### User-Defined Functions (UDFs):

Sometimes, you may need a more complex logic to fill or transform null values.

In such cases, you can define your own functions using udf:

```scala
import org.apache.spark.sql.functions.udf

val customFill = udf((value: String) => if (value == null) "custom_value" else value)

val dfCustomFilled = originalDF.withColumn("new_column", customFill(col("column_name")))
```

This allows you to apply custom logic when filling or transforming nulls.

### Handling Nulls in Window Functions:

When working with window functions, null values can impact the results. You can use the ignoreNulls option to handle them:

```scala
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("partition_column").orderBy("order_column")

val dfWithRank = originalDF.withColumn("rank", rank().over(windowSpec).ignoreNulls())
```

This example uses the rank window function, and ignoreNulls helps in handling nulls gracefully.

### Handling Nulls in Machine Learning Pipelines:

Dealing with nulls in feature columns is crucial for machine learning.

Spark ML provides a Imputer transformer to fill null values in feature columns:

```scala
import org.apache.spark.ml.feature.Imputer

val imputer = new Imputer()
  .setInputCols(Array("feature_column1", "feature_column2"))
  .setOutputCols(Array("imputed_feature_column1", "imputed_feature_column2"))
  .setStrategy("mean")

val model = imputer.fit(originalDF)
val dfImputedFeatures = model.transform(originalDF)
```

This is particularly useful when preparing data for machine learning models.

### Handling Nested Data Structures:

If your DataFrame contains nested structures like arrays or maps, handling nulls can be more intricate. 

Spark provides functions like explode, inline, and getItem for working with such structures.

```scala
val dfExploded = originalDF.select("column_name", explode(col("nested_array")).as("exploded_column"))
```

This example explodes an array column into separate rows.

```scala
%scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Create a Spark session
val spark = SparkSession.builder
  .appName("Explode Example")
  .master("local[2]")  // Use local mode for simplicity
  .getOrCreate()

// Create a sample DataFrame
val data = Seq(
  ("A", Seq(1, 2, 3)),
  ("B", Seq(4, 5)),
  ("C", Seq(6))
)

// Define the schema
val schema = List("column_name", "nested_array")

// Create the originalDF
val originalDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

// Show the original DataFrame
originalDF.show()

// Apply explode to create dfExploded
val dfExploded: DataFrame = originalDF.select(col("column_name"), explode(col("nested_array")).as("exploded_column"))

// Show the exploded DataFrame
dfExploded.show()
```

This is the output in DataBricks

```
+-----------+------------+
|column_name|nested_array|
+-----------+------------+
|          A|   [1, 2, 3]|
|          B|      [4, 5]|
|          C|         [6]|
+-----------+------------+

+-----------+---------------+
|column_name|exploded_column|
+-----------+---------------+
|          A|              1|
|          A|              2|
|          A|              3|
|          B|              4|
|          B|              5|
|          C|              6|
+-----------+---------------+
```

## 3.4. Type-Safe Data Processing: Datasets


## 3.5. Datasets, Part 2 + Exercise



# 4. Spark SQL

## 4.1. Spark as a "Database" with Spark SQL Shell

## 4.2. Spark SQL

## 4.3. Spark SQL: Exercises


# 5. Low-Level Spark.

## 5.1. RDDs

## 5.2. RDDs, Part 2 + Exercises






