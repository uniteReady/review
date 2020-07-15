package com.tianya.bigdata.hadoop.day20200708;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class MockData {

    public static void main(String[] args) throws Exception {
        String[] words = new String[]{"pk","xingxing","ruoze"};
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("out/ruoze.txt"))));

        Random random = new Random();

        for (int i = 0; i < 1000000; i++) {
            for(int j = 0; j< 30; j++){
                writer.write(words[random.nextInt(words.length)]);
                writer.write(",");
            }
        }




        writer.flush();
        writer.close();

    }
}
