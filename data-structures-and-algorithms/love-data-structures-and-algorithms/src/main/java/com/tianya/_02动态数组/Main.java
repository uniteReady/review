package com.tianya._02动态数组;

public class Main {

    public static void main(String[] args) {
        DynamicArray dynamicArray = new DynamicArray();
//        int i = dynamicArray.get(-10);
        dynamicArray.add(99);
        dynamicArray.add(88);
        dynamicArray.add(77);
        dynamicArray.add(66);

        dynamicArray.add(4,999);
        System.out.println(dynamicArray);
    }
}
