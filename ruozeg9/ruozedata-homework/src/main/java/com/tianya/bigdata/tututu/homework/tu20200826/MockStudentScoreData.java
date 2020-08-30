package com.tianya.bigdata.tututu.homework.tu20200826;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class MockStudentScoreData {

    public static void main(String[] args) throws Exception{
        StudentScore[] studentScores = {
                new StudentScore(1,"语文",18)
                ,new StudentScore(2,"数学",25)
        };

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("out/studentScores.txt"))));

        Random random = new Random();
        int count1 = 0;
        int count2 = 0;
        for (int i = 0; i < 100000000; i++) {
            if(i % 3 == 0){
                writer.write(studentScores[random.nextInt(studentScores.length)].toString());
                writer.write("\n");
                count2++;
            }else {
                writer.write(studentScores[0].toString());
                writer.write("\n");
                count1 ++;
            }
        }

        System.out.println("倾斜数据："+count1);
        System.out.println("随机数据："+count2);

        writer.flush();
        writer.close();

    }


}

class StudentScore{

    private int subjectId;

    private String subject;

    private float score;

    public StudentScore() {
    }

    public StudentScore(int subjectId, String subject, float score) {
        this.subjectId = subjectId;
        this.subject = subject;
        this.score = score;
    }

    public int getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(int subjectId) {
        this.subjectId = subjectId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return subjectId + "\t" + subject +"\t" +score;
    }
}
