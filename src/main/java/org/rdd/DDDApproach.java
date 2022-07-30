package org.rdd;

import domain.Department;
import domain.Employee;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class DDDApproach {

    //domain driven design
    public static void example(JavaSparkContext javaSparkContext) {

        JavaRDD<String> employeeRDD = javaSparkContext.textFile("file:///C:/Users/lenovo/IdeaProjects/MySpark/src/main/resources/Employee.csv");
        JavaRDD<String> deptRDD = javaSparkContext.textFile("file:///C:/Users/lenovo/IdeaProjects/MySpark/src/main/resources/Department.csv");

        //remove header and empty from data
        JavaRDD<String> employeeWithoutHeader = getCleanRDD(employeeRDD);
        JavaRDD<String> departmentWithoutHeader = getCleanRDD(deptRDD);
        JavaRDD<Employee> employeeRDDWithoutJunk = employeeWithoutHeader.map(new Function<String, Employee>() {
            @Override
            public Employee call(String s) throws Exception {

                String[] split = s.trim().split(",");
                Employee customer = new Employee();
                customer.setEmpId(Integer.parseInt(split[0]));
                customer.setName(split[1]);
                customer.setAge(Integer.parseInt(split[2]));
                customer.setDept(Integer.parseInt(split[3]));
                customer.setSalary(Integer.parseInt(split[4]));
                return customer;
            }
        });

        JavaRDD<Department> departmentRDD = departmentWithoutHeader.map(new Function<String, Department>() {
            @Override
            public Department call(String s) throws Exception {

                String[] split = s.trim().split(",");
                Department dept = new Department();
                dept.setDeptId(Integer.parseInt(split[0]));
                dept.setDeptName(split[1]);
                return dept;
            }
        });

        //findUniqueEmployees(employeeRDDWithoutJunk);
        //findTop3SalaryOfEmp(employeeRDDWithoutJunk);
        wordCountExample(employeeRDD);
    }

    private static void findTop3SalaryOfEmp(JavaRDD<Employee> emp) {

        //implemented comparable interface in Employee class
        List<Employee> top = emp.sortBy(t -> t.getSalary(), false, 1).top(3);

        System.out.println("top 3 sal:");
        top.forEach(t -> System.out.println(t.getName()));
    }

    private static void wordCountExample(JavaRDD<String> rdd) {

        JavaRDD<String> names = rdd.map(t -> t.trim().split(",", -1)[1]).filter(t-> t!=null && !t.isEmpty());
        JavaPairRDD<String, Integer> javaPairRDD = names.mapToPair(t -> new Tuple2(t, 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = javaPairRDD.reduceByKey((a, b) -> a + b);

        System.out.println("word count example");
        stringIntegerJavaPairRDD.foreach(t -> System.out.println(t._1 + ":" + t._2));

    }

    private static void findUniqueEmployees(JavaRDD<Employee> emp) {
        //remove repeated records and apply filter
        //JavaRDD<Employee> filter = emp.filter(t -> t.getAge() > 30).distinct());
        //System.out.println("filter by age:");
        //filter.foreach(t -> System.out.println(t.getName() + ";" + t.getAge() + ";" + t.getEmpId()));

        JavaPairRDD<String, Employee> employeeJavaPairRDD = emp.mapToPair(t -> new Tuple2<>(t.getName(), t));

        employeeJavaPairRDD = employeeJavaPairRDD.reduceByKey((a, b) -> a);

        System.out.println("unique records");
        employeeJavaPairRDD.values().foreach(t -> System.out.println(t.getName() + ";" + t.getAge() + ";" + t.getEmpId()));
    }

    private static JavaRDD<String> getCleanRDD(JavaRDD<String> rdd) {

        //remove junk and duplicate data
        JavaRDD<String> cleanRdd = rdd
                .filter(t -> t != null && !t.startsWith("EmpId"))
                .filter(t -> t != null && !t.startsWith("DeptId"))
                .filter(t -> t != null && !t.trim().startsWith(","))
                .distinct();

        cleanRdd.foreach(t -> System.out.println(t));

        return cleanRdd;
    }
}
