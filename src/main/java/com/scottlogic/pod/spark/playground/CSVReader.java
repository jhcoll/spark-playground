package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVReader {
    private SparkSession spark;
    // private JavaSparkContext jsc;

    CSVReader(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        // this.jsc = jsc;
    }

    public void run() {

        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("Index",  DataTypes.IntegerType, false),
            DataTypes.createStructField("User Id", DataTypes.StringType, false),
            DataTypes.createStructField("First Name", DataTypes.StringType, false),
            DataTypes.createStructField("Last Name", DataTypes.StringType, false),
            DataTypes.createStructField("Sex", DataTypes.StringType, false),
            DataTypes.createStructField("Email", DataTypes.StringType, false),
            DataTypes.createStructField("Phone", DataTypes.StringType, false),
            DataTypes.createStructField("Date of Birth", DataTypes.DateType, false),
            DataTypes.createStructField("Job Title", DataTypes.StringType, false)
        });

        Dataset<Row> peopleDF = spark.read()
                .schema(schema)
                .option("header", "true")
                .csv("./data/people-100000.csv")
                // .repartition(64)
                .toDF();

        peopleDF.printSchema();

        // UserDefinedFunction calcAge = functions.udf((Date i) -> {
        //     LocalDate date = new java.sql.Date(i.getTime()).toLocalDate();
        //     return Period.between(date, LocalDate.now()).getYears();
        // }, DataTypes.IntegerType);
        
        // Dataset<Row> withAge = cabsDF.withColumn("Age", calcAge.apply(cabsDF.col("Date of Birth")));

        // Dataset<Row> ageByJobTitle = withAge.groupBy("Job Title").agg(functions.sum("Age").alias("Total of Ages"));
        // ageByJobTitle.show();

        System.out.println("Number of partitions: " + peopleDF.rdd().getNumPartitions());

        peopleDF.join(peopleDF, peopleDF.col("User Id").equalTo(peopleDF.col("User Id"))).show();
        
        spark.sql("CREATE DATABASE IF NOT EXISTS PEOPLE");
        peopleDF.createOrReplaceTempView("peopledb");
        spark.catalog().setCurrentDatabase("PEOPLE");
        // peopleDF.write().saveAsTable("peopledb");
        // spark.sql("SHOW DATABASES").show();

        // spark.sql("SELECT * FROM peopleWithAge").show();

        // System.out.println(spark.catalog().databaseExists("PEOPLE"));
        // spark.catalog().listTables().show();

        Dataset<Row> jobCount = peopleDF.groupBy("Job Title").count();
        jobCount.orderBy(jobCount.col("count").desc()).show();

        spark.sql("SELECT * FROM people").show();
    }
}
