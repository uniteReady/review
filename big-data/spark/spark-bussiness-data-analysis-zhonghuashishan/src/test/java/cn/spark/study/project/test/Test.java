package cn.spark.study.project.test;

public class Test {

    @org.junit.Test
    public void testSingleton(){
        Singleton instance = Singleton.getInstance();
        Singleton instance2 = Singleton.getInstance();
        System.out.println(instance);
        System.out.println(instance2);
    }
}
