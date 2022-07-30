package domain;

import java.io.Serializable;
import java.util.Objects;

public class Employee implements Serializable, Comparable<Employee> {

    private int empId;
    private String name;
    private int age;

    private int dept;
    private int salary;

    public void setEmpId(int empId) {
        this.empId = empId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getEmpId() {
        return empId;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }


    public int getDept() {
        return dept;
    }

    public void setDept(int dept) {
        this.dept = dept;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee customer = (Employee) o;
        return empId == customer.empId && age == customer.age && Objects.equals(name, customer.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(empId, name, age);
    }

    @Override
    public int compareTo(Employee o) {
        return this.getSalary() - o.getSalary();
    }
}
