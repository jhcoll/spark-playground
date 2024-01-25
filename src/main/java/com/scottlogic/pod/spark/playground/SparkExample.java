package com.scottlogic.pod.spark.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkExample {
    private static String csvFilePath;

    public static void setCsvFilePath(String filePath) {
        csvFilePath = filePath;
    }

    public static long processData() {
        // Implement your data processing logic here
        // ...

        // Example: Read CSV and return the DataFrame
        SparkSession spark = SparkSession.builder()
                .appName("SparkExample")
                .config(new SparkConf().setMaster("local[*]"))
                .getOrCreate();

        return spark.read().csv(csvFilePath).toDF().count();
    }
}
