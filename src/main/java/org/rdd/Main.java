package org.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
                "Dataset_Basic");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        DDDApproach.example(javaSparkContext);
        javaRddDemo(javaSparkContext);
    }

    private static void javaRddDemo(JavaSparkContext javaSparkContext) {


        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("file:///C:/Users/lenovo/IdeaProjects/MySpark/src/main/resources/Customer.csv");

        //print all records including header
        stringJavaRDD.foreach(t -> System.out.println(t));

        //fetch header
        String firstElement = stringJavaRDD.first();
        System.out.println("file header :" + firstElement);

        //remove header and empty from data
        JavaRDD<String> customerWithoutHeader = stringJavaRDD
                .filter(t -> t != null && !t.startsWith("Contact"))
                .filter(t -> t != null && !t.trim().startsWith(","));
        customerWithoutHeader.foreach(t -> System.out.println(t));

        //convert to <K,V> pair
        JavaPairRDD<Integer, String> customerJavaPairRDD = customerWithoutHeader.mapToPair(t -> {
            String[] split = t.trim().split(",");
            Tuple2<Integer, String> tuple = new Tuple2(split[0], split[1]);
            return tuple;
        });

        //As key values pair
        customerJavaPairRDD.foreach(t -> System.out.println("contact:" + t._1 + ";name:" + t._2));

        //changing key
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = customerJavaPairRDD.mapToPair(t -> new Tuple2<>(t._2, t._1));
        stringIntegerJavaPairRDD.foreach(t -> System.out.println("name:" + t._1 + ";contact:" + t._2));

        //print unique name only with any contact
        //JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey((a, b) -> a);
        //stringIntegerJavaPairRDD1.foreach(t -> System.out.println("contact:" + ";name:" ));

        Map<String, Integer> stringIntegerMap = stringIntegerJavaPairRDD.collectAsMap();

        System.out.println("unique values:");
        for (Map.Entry s : stringIntegerMap.entrySet()) {
            System.out.println(s.getValue() + "," + s.getKey());
        }
    }
}