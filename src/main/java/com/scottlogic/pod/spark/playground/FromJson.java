package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FromJson {
    private SparkSession spark;
    private JavaSparkContext jsc;

    FromJson(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void read() {

        Dataset<Row> people = spark.read().json("./data/profiles.json");

        people.printSchema();

        List<String> jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
        // anotherPeople.show();
        anotherPeople.printSchema();


        List<Integer> data = Arrays.asList(1, 2, null, 4, 5);

        JavaRDD<Integer> rdd = jsc.parallelize(data);

        String columnName = "Number";
        List<StructField> fields = Arrays
                .asList(DataTypes.createStructField(columnName, DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = rdd.map(value -> RowFactory.create(value));
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        df.printSchema();

        df.write()
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .option("delimiter", "|")
                    .option("nullValue", "")
                    .option("inferSchema", "true")
                    .csv("./data/peopleAsCsv.csv");

    }
}
