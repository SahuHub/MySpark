package org.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark_Basic");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);// this is wrapper of SparkContext
        SparkContext sc = SparkContext.getOrCreate(conf);



        DDDApproach.example(javaSparkContext);
        //RddBasicOperations.javaRddDemo(javaSparkContext);
    }
}