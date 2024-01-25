package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import javafx.scene.chart.PieChart.Data;

/*
 * Covers:
 * 
 * Object and array manipulation :-
 * functions.explode(...)
 * .col(...).getField(...)
 * 
 * Data collecting :-
 * - .groupBy(...)
 * - .agg(...)
 * - functions.mean(...) / functions.avg(...)
 * - functions.mode(...)
 * 
 * Data manipulation :-
 * - .when(...)
 * - col(...).lt(...), col(...).leq(...), col(...).gt(...), col(...).geq(...)
 * - .and(...)
 * - .otherwise(...)
 * 
 * Slightly unusual :-
 * - functions.size(...)
 * - functions.slice(...)
 */

public class Spark101 {

    private SparkSession spark;
    private JavaSparkContext jsc;

    Spark101(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void run() {

        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json("./data/school-classes.json")
                .toDF();

        df.show();
        df.printSchema();

        /*
         * Task:
         * Using the DF provided, create a new table of the student objects and their
         * school year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.explode(...)
         */

        Dataset<Row> exploded = df.select(functions.explode(df.col("students")).as("student"), df.col("yearNum").as("year"));
        exploded.show();

        /*
         * Task:
         * Building off of the table just created, seperate each student object so that
         * you now have a table of student names, ages and school years
         * 
         * The functions that you may/will need to use for this are:
         * - .col(...).getField(...)
         */

        Dataset<Row> exploded2 = exploded.select(exploded.col("student").getField("studentName").as("name"),
                exploded.col("student").getField("studentAge").as("age"), exploded.col("year"));
        exploded2.show();
        /*
         * Task:
         * Following on, what is the mean age and the mode age of each school year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.groupBy(...)
         * - .agg(...)
         * - functions.mean(...) / functions.avg(...)
         * - functions.mode(...)
         */
        Dataset<Row> meanAge = exploded2.groupBy("year").agg(functions.mean("age").as("meanAge"), functions.mode(exploded2.col("age")).as("modeAge")).orderBy("year");
        meanAge.show();
        /*
         * Task:
         * Please create a new Table of the school teachers and the Key stage they teach
         * 
         * https://thinkstudent.co.uk/school-year-groups-key-stages/
         * 
         * The functions that you may/will need to use for this are:
         * - .when(...)
         * - col(...).lt(...), col(...).leq(...), col(...).gt(...), col(...).geq(...)
         * - .and(...)
         * - .otherwise
         */
        Dataset<Row> teachers = df.select(df.col("teacherName"), df.col("yearNum"),
                functions.when(df.col("yearNum").leq(2), "KS1")
                        .when(df.col("yearNum").geq(3).and(df.col("yearNum").leq(6)), "KS2")
                        .when(df.col("yearNum").geq(7).and(df.col("yearNum").leq(9)), "KS3")
                        .when(df.col("yearNum").geq(10).and(df.col("yearNum").leq(11)), "KS4")
                        .when(df.col("yearNum").geq(12).and(df.col("yearNum").leq(13)), "KS5")
                        .otherwise("Unknown").as("keyStage"));
        teachers.show();

        /*
         * Task:
         * Produce a new table containing the year, class number and class size
         * 
         * The functions that you may/will need to use for this are:
         * - functions.size(...)
         */
        Dataset<Row> classSize = df.select(df.col("yearNum"), df.col("classNum"), functions.size(df.col("students")).as("classSize"));
        classSize.show();
        /*
         * Task:
         * Finaly, using functions.slice, create a table of each year number, the class
         * number and the name of the first student listed in each class
         * 
         * The functions that you may/will need to use for this are:
         * - functions.slice(...)
         */
        Dataset<Row> sliced = df.select(df.col("yearNum"), df.col("classNum"), functions.slice(df.col("students"), 1, 1).getField("studentName").as("firstStudent"));
        sliced.show();
    }

}
