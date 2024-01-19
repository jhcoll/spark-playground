package com.scottlogic.pod.spark.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPlayground {
    private static final Logger logger = LoggerFactory.getLogger(SparkPlayground.class);

    public static void main(String[] args) {
        // Create a Spark session
        logger.info("Starting spark legend demo app");

        SparkConf conf = new SparkConf()
                .setAppName("SparkPlayground")
                // .set("spark.dynamicAllocation.enabled", "true")
                // .set("spark.executor.cores", "1")
                // .set("spark.dynamicAllocation.minExecutors","1")
                // .set("spark.dynamicAllocation.maxExecutors","16")
                // .setMaster("spark://localhost:7077"); // Switch to this to run against a local cluster
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        SparkContext cs = spark.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(cs);

        /*
         * Each of these can be uncommented to run them
         */

        // NumberCount numberCount = new NumberCount(spark, jsc);
        // numberCount.count();

        // FromJson fromJson = new FromJson(spark, jsc);
        // fromJson.read();

        // WordCount wordCount = new WordCount(spark, jsc);
        // wordCount.count();

        // CSVReader csvReader = new CSVReader(spark, jsc);
        // csvReader.run();

        // CSVtoXML csvtoXML = new CSVtoXML(spark, jsc);
        // csvtoXML.run();

        // StructuredStreamingUsage structuredStreamingUsage = new StructuredStreamingUsage(spark, jsc);
        // try {
        //     structuredStreamingUsage.run();
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }

        // // Stop the Spark session
        logger.info("Stopping spark legend demo app");
        spark.stop();
        // keepAlive();
    }

    // private static void keepAlive() {
    //     while (true) {
    //         try {
    //             Thread.sleep(1000);
    //         } catch (InterruptedException e) {
    //             e.printStackTrace();
    //         }
    //     }
    // }
}