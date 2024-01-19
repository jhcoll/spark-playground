package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class DataSchemas {

    public static List<StructField> peopleSchema = Arrays.asList(
            DataTypes.createStructField("Index",  DataTypes.IntegerType, false),
            DataTypes.createStructField("User Id", DataTypes.StringType, false),
            DataTypes.createStructField("First Name", DataTypes.StringType, false),
            DataTypes.createStructField("Last Name", DataTypes.StringType, false),
            DataTypes.createStructField("Sex", DataTypes.StringType, false),
            DataTypes.createStructField("Email", DataTypes.StringType, false),
            DataTypes.createStructField("Phone", DataTypes.StringType, false),
            DataTypes.createStructField("Date of Birth", DataTypes.DateType, false),
            DataTypes.createStructField("Job Title", DataTypes.StringType, false)
            );

    public static List<StructField> organisationSchema = Arrays.asList(
            DataTypes.createStructField("Index", DataTypes.IntegerType, false),
            DataTypes.createStructField("Organization Id", DataTypes.StringType, false),
            DataTypes.createStructField("Name", DataTypes.StringType, false),
            DataTypes.createStructField("Website", DataTypes.StringType, false),
            DataTypes.createStructField("Country", DataTypes.StringType, false),
            DataTypes.createStructField("Description", DataTypes.StringType, false),
            DataTypes.createStructField("Founded", DataTypes.IntegerType, false),
            DataTypes.createStructField("Industry", DataTypes.StringType, false),
            DataTypes.createStructField("Number of employees", DataTypes.IntegerType, false)
        );

    public static List<StructField> peopleOrganisationSchema = Arrays.asList(
            DataTypes.createStructField("User Id", DataTypes.StringType, false),
            DataTypes.createStructField("Organization Id", DataTypes.StringType, false)
    );
}
