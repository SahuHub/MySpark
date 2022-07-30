package org.rdd;

import domain.Customer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.Iterator;

public class DDDApproach {

    //domain driven design
    public static void example(JavaSparkContext javaSparkContext){

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("file:///C:/Users/lenovo/IdeaProjects/MySpark/src/main/resources/Customer.csv");

        //print all records including header
        System.out.println("inside ddd");
        stringJavaRDD.foreach(t -> System.out.println(t));

        //remove header and empty from data
        JavaRDD<String> customerWithoutHeader = stringJavaRDD
                .filter(t -> t != null && !t.startsWith("Contact"))
                .filter(t -> t != null && !t.trim().startsWith(","));
        customerWithoutHeader.foreach(t -> System.out.println(t));

        customerWithoutHeader.persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Customer> customerRdd = customerWithoutHeader.map(new Function<String, Customer>() {
            @Override
            public Customer call(String s) throws Exception {

                String[] split = s.trim().split(",");
                Customer customer = new Customer();
                customer.setContact(Integer.parseInt(split[0]));
                customer.setName(split[1]);
                customer.setAge(Integer.parseInt(split[2]));
                return customer;
            }
        });

        JavaRDD<Customer> filter = customerRdd.filter(t -> t.getAge() > 30);

        System.out.println("filter by age:");
        filter.foreach(t -> System.out.println(t.getName()));


    }
}
