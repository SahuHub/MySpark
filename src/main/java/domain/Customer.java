package domain;

public class Customer {

    private int contact;
    private String name;

    private int age;

    public void setContact(int contact){
        this.contact = contact;
    }

    public void setName(String name){
        this.name = name;
    }

    public int getContact(){
        return contact;
    }

    public String getName(){
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
