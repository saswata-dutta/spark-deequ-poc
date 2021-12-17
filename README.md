### Tips
Use Amazon Deequ to test data quality before persisting the Dataframe.

https://github.com/awslabs/deequ

 
### Running
`sbt run`


### Alternative without any external dependency

Can use an UDF to define expectations per table (dataframe/dataset), and count num of valid rows.

```scala

val df = spark.createDataFrame(Seq(
        (1, "xx"),
        (6, "xy"),
        (10, "ab"),
        (11, "xa")
    )).toDF("x", "y")

val expectation = udf((x: Int, y: String) => {
        x > 5 && x < 10 && y.startsWith("x")
    })

val df_with_status = df.withColumn("valid", expectation($"x", $"y"))

// Alternately if checks are simple might be able to achieve via col exprs
val simple_validations = col("x") > 5 && col("x") < 10 && col("y").startsWith("x")
val df_with_status1 = df.withColumn("valid", simple_validations)


val counts = df_with_status.groupBy("valid").count().collect().
    map(r => (r(0).asInstanceOf[Boolean], r(1).asInstanceOf[Long])).toMap
// Map(false -> 3, true -> 1)

val df_clean = df_with_status.filter($"valid").drop("valid")
/*
+---+---+
|  x|  y|
+---+---+
|  6| xy|
+---+---+
*/

```
### TODO

Deequ currently only seems to work with scala 2.11.10, and datarame not dataset.

