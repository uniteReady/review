package com.tianya.bigdata.homework.day20200812;

import org.junit.Test;

public class MockAccessLogTest {


    @Test
    public void testMockUrl(){
        for (int i = 0; i < 904000; i++) {
            System.out.println(MockAccessLog.mockUrl());
        }
    }



    @Test
    public void testMockcacheStatus(){
        for (int i = 0; i < 100; i++) {
            System.out.println(MockAccessLog.mockcacheStatus());
        }
    }

    @Test
    public void testMockResponseTime(){
        int counter = 0;
        String responseSize = "";
        for (int i = 0; i < 904000; i++) {
            if (i % 17 == 0 && counter < 904) {
                responseSize = "-";
                counter++;
            } else {
                responseSize = MockAccessLog.mockIntegerData() + "";
            }
        }
        System.out.println(counter);


    }
}
