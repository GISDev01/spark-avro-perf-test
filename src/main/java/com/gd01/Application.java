package com.gd01;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Application {
    static Logger logger = Logger.getLogger(Application.class.getName());

    public static void main(String[] args) {
        String logConfigFilepath = "src/main/resources/log4j.properties";
        PropertyConfigurator.configure(logConfigFilepath);

        SparkSession spark = SparkSession
                .builder()
                .appName("spark-avro-perf-test")
                .master("local[4]")
                .getOrCreate();

        String dataFilepath = "src/main/resources/Crimes_June2020.csv";

        StructType crimeSchema = new StructType()
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

        Dataset<Row> rawDf = spark.read()
                .option("mode", "DROPMALFORMED")
                .option("header", true)
                .schema(crimeSchema)
                .csv(dataFilepath);

        Dataset<Row> beatCountDf = rawDf
                .groupBy("Beat")
                .agg(sum("Beat").alias("BeatSumCount"))
                .sort(desc("BeatSumCount"));

        beatCountDf.show(5);

        Dataset<Integer> yearsDf = rawDf.map(
                (MapFunction<Row, Integer>) row ->
                        row.<Integer>getAs("Year"),
                Encoders.INT());
        yearsDf.show(5);

        Dataset<String> primaryTypeDf = rawDf.map(
                (MapFunction<Row, String>) row ->
                        row.<String>getAs("Primary Type"),
                Encoders.STRING());
        primaryTypeDf.show(5);

        rawDf.filter("Year = 2020").show(5);
        rawDf.filter("`Primary Type` = 'ASSAULT'").show(5);

        Dataset<Integer> newYears = yearsDf.flatMap((FlatMapFunction<Integer, Integer>) year -> {
            return Arrays.asList(year + 1, year + 2).iterator();
        }, Encoders.INT());

        newYears.show(5);

//        df
//                .foreach((ForeachFunction<Row>) row ->
//                {
//                    int pTypeIndex = row.fieldIndex("Primary Type");
//                    System.out.println(row.get(pTypeIndex));
//                });

        spark.stop();
    }
}
