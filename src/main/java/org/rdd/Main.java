package org.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
                "Dataset_Basic");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        DDDApproach.example(javaSparkContext);
        RddBasicOperations.javaRddDemo(javaSparkContext);
    }
}