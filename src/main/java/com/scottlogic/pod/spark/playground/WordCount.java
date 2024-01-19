package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class WordCount {
    private SparkSession spark;
    private JavaSparkContext jsc;

    WordCount(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void count() {
        JavaRDD<String> textFile = jsc.textFile("data/example-words.txt");

        /**
         * Task: Modify this to clean the data- remove any junk, fix capitalisation
         */

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap((s) -> {
                        s = s.replaceAll("[!,.]", "");
                        s = s.toLowerCase();
                        return Arrays.asList(s.split(" ")).iterator();
                })
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        // counts.foreach(tuple -> System.out.println(tuple));

        /**
         * Task: Complete/fix the following code to create a DataSet from the counts
         * with the
         * follow schema with "word" and "count" columns
         */

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("word", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.IntegerType, true)
        );

        StructType schema = DataTypes.createStructType(fields);

        List<Row> rows = counts
                .map(tuple -> RowFactory.create(tuple._1(), tuple._2()))
                .collect();

        Dataset<Row> df = spark.createDataFrame(rows, schema); 
        df.show();

        /**
         * Task: Find and show the most frequent word
         */
        Row highestCount = df.orderBy(df.col("count").desc()).first();
        System.out.println("The most frequent word is: " + highestCount.getString(0) + " with a count of: " + highestCount.getInt(1));

        /**
         * Task: Count the total words beginning with "a"
         */

        long count = df.filter(df.col("word").startsWith("a")).count();
        System.out.println("count of words beginning with a: " + count);

    }

}
