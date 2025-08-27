package org.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("Spark_Basic")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);// this is wrapper of SparkContext
        SparkContext sc = SparkContext.getOrCreate(conf);

        SparkSession spark = SparkSession.builder()
                .appName("ReadCSVExample")
                .master("local[*]")  // run in local mode
                .getOrCreate();


        //DDDApproach.example(javaSparkContext);
        //RddBasicOperations.javaRddDemo(javaSparkContext);

        Dataset<Row> df = spark.read()
                .option("header", "true")   // use first line as header
                .option("inferSchema", "true") // auto-detect column types
                .csv("file:///C:/Users/Ashish/IdeaProjects/MySpark/src/main/resources/Employee.csv");

        df.show();  // display in console
    }
}