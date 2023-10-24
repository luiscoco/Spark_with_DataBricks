# Udemy Apache Spark 3. Big Data Essentials in Scala Rock the JVM

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

## 2.3. How DataFrames Work

## 2.4. Data Sources

## 2.5. Data Sources. Exercises

## 2.6. DataFrames Columns and Expressions

## 2.7. DataFrames Columns and Expressions. Exercises

## 2.8. DataFrame Aggregations

## 2.9. DataFrame Joins

## 2.10. DataFrame Joins. Exercises



# 3. Spark Types and Datasets

. asdf




# 4. Spark SQL

.a asdf



## 5. Low-Level Spark.

.adfa







