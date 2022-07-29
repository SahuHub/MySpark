package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
                "Dataset_Basic");

        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> customer = session.read().option("header","true").csv("file:///C:/Users/lenovo/IdeaProjects/MySpark/src/main/resources/Customer.csv");

        Dataset<Row> name = customer.select("Name");

        Encoder<Tuple2<String, Integer>> tuple = Encoders.tuple(Encoders.STRING(), Encoders.INT());

        JavaPairRDD<Row, Integer> rowIntegerJavaPairRDD = name.toJavaRDD().mapToPair(t -> new Tuple2<Row, Integer>(t, 1));
        JavaPairRDD<Row, Integer> rowIntegerJavaPairRDD1 = rowIntegerJavaPairRDD.reduceByKey((a, b) -> a + b);

        rowIntegerJavaPairRDD1.foreach(t -> System.out.println(t._1.mkString() + " " + t._2.intValue()));


        Dataset<Tuple2<String, Integer>> map1 = name.map(t -> new Tuple2<String, Integer>(t.mkString(), 1), tuple);

       // map1.show();
    }
}