package com.tianya.bigdata.tututu.homework.tu20200826;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentSubjectScore implements WritableComparable {
    private int subjectId;

    private String studentName;

    private String subject;

    private String score;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(subjectId);
        out.writeUTF(studentName);
        out.writeUTF(subject);
        out.writeUTF(score);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.subjectId = in.readInt();
        this.studentName = in.readUTF();
        this.subject = in.readUTF();
        this.score = in.readUTF();
    }

    public StudentSubjectScore() {
    }

    public StudentSubjectScore(int subjectId, String studentName, String subject, String score) {
        this.subjectId = subjectId;
        this.studentName = studentName;
        this.subject = subject;
        this.score = score;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public int getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(int subjectId) {
        this.subjectId = subjectId;
    }

    @Override
    public String toString() {
        return subjectId + "\t" + studentName + "\t" + subject + "\t" + score;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
