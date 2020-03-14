package com.atguigu.bean;

import java.util.Objects;

public class Student {

    private String class_id;
    private String name;
    private int age;
    private double score;
    private String hobby;

    public Student(){}

    public Student(String class_id, String name, int age, double score, String hobby) {
        this.class_id = class_id;
        this.name = name;
        this.age = age;
        this.score = score;
        this.hobby = hobby;
    }

    public String getClass_id() {
        return class_id;
    }

    public void setClass_id(String class_id) {
        this.class_id = class_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getHobby() {
        return hobby;
    }

    public void setHobby(String hobby) {
        this.hobby = hobby;
    }

    @Override
    public String toString() {
        return "Student{" +
                "class_id='" + class_id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", score=" + score +
                ", hobby='" + hobby + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Student student = (Student) o;
        return age == student.age &&
                Double.compare(student.score, score) == 0 &&
                Objects.equals(class_id, student.class_id) &&
                Objects.equals(name, student.name) &&
                Objects.equals(hobby, student.hobby);
    }

    @Override
    public int hashCode() {
        return Objects.hash(class_id, name, age, score, hobby);
    }
}
