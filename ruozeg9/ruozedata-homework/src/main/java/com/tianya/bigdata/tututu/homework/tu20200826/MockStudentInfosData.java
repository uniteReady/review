package com.tianya.bigdata.tututu.homework.tu20200826;

import java.io.*;

public class MockStudentInfosData {

    public static void main(String[] args) throws Exception {
        StudentInfo[] studentInfos= {
                new StudentInfo(1,"张三",1)
                ,new StudentInfo(3,"王五",2)
//                ,new StudentInfo(4,"赵六",2)
        };
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("out/studentInfos.txt"))));

        for (int i = 0; i < 2; i++) {
            writer.write(studentInfos[i].toString());
            writer.write("\n");
        }

        writer.flush();
        writer.close();
        
    }
    
    
}


/**
 *
 */
class StudentInfo{
    private int id;
    
    private String studentName;
    
    private int subjectId;

    public StudentInfo() {
    }

    public StudentInfo(int id, String studentName, int subjectId) {
        this.id = id;
        this.studentName = studentName;
        this.subjectId = subjectId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public int getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(int subjectId) {
        this.subjectId = subjectId;
    }

    @Override
    public String toString() {
        return id + "\t" +studentName + "\t" + subjectId;
    }
}
