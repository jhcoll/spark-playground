package com.scottlogic.pod.spark.playground.fitnesse.fixtures;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.scottlogic.pod.spark.playground.WordCount;

public class WordCountFixtureTest {
    String filepath;
    long result;

    public long getResult() {
        return result;
    }

    public void setResult(long result) {
        this.result = result;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public void execute() {
        SparkSession spark = SparkSession.builder().appName("Word Count").master("local").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        WordCount wordcount  = new WordCount(spark, jsc);

        result = wordcount.countDistinctWords(filepath);
    }
}
