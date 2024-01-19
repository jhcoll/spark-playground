package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredStreamingUsage {
        private SparkSession spark;
    // private JavaSparkContext jsc;

    StructuredStreamingUsage(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        // this.jsc = jsc;
    }

    public void run() throws TimeoutException, StreamingQueryException {
        Dataset<Row> inputData = spark.readStream()
                                .format("socket")
                                .option("host", "localhost")
                                .option("port", 9999)
                                .load();

        Dataset<String> words = inputData.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                                .outputMode("complete")
                                .format("console")
                                .start();
                                
        query.awaitTermination();
    }   
}
