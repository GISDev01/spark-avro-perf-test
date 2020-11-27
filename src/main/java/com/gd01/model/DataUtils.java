package com.gd01.model;

import org.apache.spark.sql.types.StructType;

public class DataUtils {

    public static String csvDataFilepath = "src/main/resources/Crimes_June2020.csv";
    public static String avroDataFilepath = "src/main/resources/Crimes_June2020.avro";

    public static StructType crimeSchema = new StructType()
            .add("ID", "string")
            .add("Case Number", "string")
            .add("Date", "string")
            .add("Block", "string")
            .add("IUCR", "string")
            .add("Primary Type", "string")
            .add("Description", "string")
            .add("Location Description", "string")
            .add("Arrest", "string")
            .add("Domestic", "string")
            .add("Beat", "string")
            .add("District", "string")
            .add("Ward", "string")
            .add("Community Area", "string")
            .add("FBI Code", "string")
            .add("X Coordinate", "string")
            .add("Y Coordinate", "string")
            .add("Year", "integer")
            .add("Updated On", "string")
            .add("Latitude", "string")
            .add("Longitude", "string")
            .add("Location", "string");

    public static String getCsvDataFilepath() {
        return csvDataFilepath;
    }

    public static StructType getCrimeSchema() {
        return crimeSchema;
    }

}
