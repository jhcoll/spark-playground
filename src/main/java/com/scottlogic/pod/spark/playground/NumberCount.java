package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class NumberCount {
    private SparkSession spark;
    private JavaSparkContext jsc;

    NumberCount(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void count() {
        /*
         * Here we create an RDD with a list of numbers
         * An RDD (Resilient Distributed Dataset) is a fundamental data
         * structure that represents a distributed collection of data elements that can
         * be processed in parallel. RDDs are the building blocks of Spark and provide
         * an abstraction for distributed data processing.
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = jsc.parallelize(data);

        /*
         * We can perform transformations and actions on RDDs
         * Like rdd.map, rdd.filter or rdd.reduce
         */

        /*
         * Task: map the values to themselves plus one
         */
        JavaRDD<Integer> rddPlusOne = rdd.flatMap(value -> Arrays.asList(value + 1).iterator());

        /*
         * Task: total up the values in the rddPlusOne
         */
        Integer sum = 0;
        sum = jsc.parallelize(rddPlusOne.collect()).reduce((a, b) -> a + b);
        System.out.println("The sum of rddPlusOne is = " + sum);

        /*
         * Now we turn the RDD into a Dataset.
         * A Dataset is an abstraction layer on top of RDDs.
         * 
         * It represents a distributed collection of data organized into
         * named columns. It provides a type-safe, object-oriented programming interface
         * and is designed to provide the benefits of both the structured data
         * processing capabilities of DataFrames and the type safety of RDDs.
         */

        String columnName = "Number";
        List<StructField> fields = Arrays
                .asList(DataTypes.createStructField(columnName, DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = rddPlusOne.map(value -> RowFactory.create(value));
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        df.printSchema();

        // df.show();

        List<String> jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        anotherPeople.show();
        anotherPeople.printSchema();


        /**
         * We can perform the same actions and transformations as on the RDD, but use a
         * different API to do so.
         * 
         * One way is through user defined functions.
         * 
         * Task: Define a Spark Java function return the square of the number and use it
         * to create a new column of squared values
         * 
         */
        // UserDefinedFunction squareUDF = functions.udf((Integer x) -> {
        //     return x * x;
        // }, DataTypes.IntegerType);

        // spark.udf().register("intToDouble", (UDF1<Integer, Double>) (x) -> {
        //     return Double.valueOf(x.toString());
        // }, DataTypes.DoubleType);

        // spark.udf().register("doubleToInt", (UDF1<Double, Integer>) (x) -> {
        //     return x.intValue();
        // }, DataTypes.IntegerType);

        // df = df.withColumn("value squared", squareUDF.apply(df.col(columnName)));
        // df = df.withColumn("value as double", functions.callUDF("intToDouble", df.col(columnName)));
        // df = df.withColumn("double as int", functions.callUDF("doubleToInt", df.col("value as double")));
        
        
        // df.show();
        // df.printSchema();
        
        /**
         * Task: Now calculate the total of the squared column from the DataSet using
         * the dataset api
         */

        // Row total =
        // System.out.println(total);
        // Row total = df.agg(functions.sum("value_squared")).first();
        // System.out.println("the total of the squared values is = " + total.get(0));

        /*
         * While user defined functions allow us to do just about anything, they are a
         * block box to spark.
         * So it cannot optimise them during use, making them poor for performance.
         * 
         * Instead spark provides functions that can do most operations that we could
         * want
         * 
         * Task: Use native spark functions to produce another value_squared_with_spark
         * column
         */

        // df = df.withColumn("value_squared_with_function", df.col("value").multiply(df.col("value")));
        // df.show();

        /**
         * Task: And recalculate your total. See if they match 
         */

    }
}
