package com.tianya.bigdata.homework.day20200719;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class MockData1 {
    public static final String PREFIX_STR = "http://ruozedata.com/question";
    public static final String SUFFIX_STR = ".html";
    public static final  String SEPARATOR = "/";
    public static final String PARAMETER = "?a=b&c=d";

    public static void mockQuestionData() throws Exception{
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("out/question1.txt")));
        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            int questionId = random.nextInt(1000);
            writer.write(PREFIX_STR+questionId+"\n");

        }
        writer.flush();
        writer.close();
    }

    public static void mockCourseNoData() throws Exception{
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("out/course_no.txt")));
        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            String courseUrl = PREFIX_STR;
            int courseNo1 = random.nextInt(50000);
            courseUrl += SEPARATOR+courseNo1;
            if(courseNo1 % 2 ==1){
                //如果是奇数，那么给他一门子课程
                int subCourseNo = random.nextInt(20);
                courseUrl += SEPARATOR + subCourseNo;
            }
            //拼接上后缀
            courseUrl += SUFFIX_STR;
            if(courseNo1 % 3 == 0){
                //如果课程是3的倍数，就给一串参数
                courseUrl += PARAMETER;
            }
            writer.write(courseUrl+"\n");
        }



        writer.flush();
        writer.close();
    }


    public static void main(String[] args) throws Exception{
        mockCourseNoData();
    }
}
