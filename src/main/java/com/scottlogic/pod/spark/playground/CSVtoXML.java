package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVtoXML {
    private SparkSession spark;
    // private JavaSparkContext jsc;

    CSVtoXML(SparkSession spark, JavaSparkContext jsc) {
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
                .toDF();

        Dataset<Row> olderThan30 = peopleDF.filter(peopleDF.col("Date of Birth").lt("1989-01-01"))
                                        .filter(peopleDF.col("first name").equalTo("Stefanie"))
                                        .filter(peopleDF.col("Sex").equalTo("Female"))
                                        .filter(peopleDF.col("Job Title").equalTo("Social worker"));

        System.out.println(olderThan30.count());
        olderThan30.show();

        olderThan30.write()
                    .format("xml")
                    .option("rootTag", "people")
                    .option("rowTag", "person")
                    .save("./data/second.xml");
    }

}
