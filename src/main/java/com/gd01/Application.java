package com.gd01;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import static org.apache.spark.sql.functions.col;

public class Application {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("spark-avro-perf-test")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> crimeDataset = spark
                .read()
                .option("header", true)
                .csv("src/main/resources/Crimes_June2020.csv");

        // crimeDataset.filter("Primary Type = 'HOMICIDE'").show();


        // crimeDataset.show();
        // crimeDataset.printSchema();

        crimeDataset
                .foreach((ForeachFunction<Row>) row ->
                {
                    //Integer index = row.fieldIndex("accountNumber");
                    int pTypeIndex = row.fieldIndex("Primary Type");
                    // System.out.println(row.get(pTypeIndex));
                });


        spark.close();
    }
}
