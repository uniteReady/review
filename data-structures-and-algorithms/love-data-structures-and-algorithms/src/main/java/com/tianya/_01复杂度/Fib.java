package com.tianya._01复杂度;

/**
 * 斐波那契数列
 * 0  1  1  2  3  5  8
 */
public class Fib {

    /**
     * 递归层次太深会有性能问题
     * @param n
     * @return
     */
    public static  int fib1(int n){
        if(n<= 1){ return  n;}
        return fib1(n-1)+fib1(n-2);
    }

    public static int fib2(int n){
        if(n <= 1){return  n;}
        int first = 0;
        int second = 1;

        for(int i = 0;i < n-1; i++){
            int sum =first + second;
            first = second;
            second = sum;
        }
        return second;
    }


    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(fib2(72));
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}
