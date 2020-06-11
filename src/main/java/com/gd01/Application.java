package com.gd01;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("spark-avro-perf-test")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("src/main/resources/Crimes_June2020.csv");

        df.show();

        spark.close();
    }
}
