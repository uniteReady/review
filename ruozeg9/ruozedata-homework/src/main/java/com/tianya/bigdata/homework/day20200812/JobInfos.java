package com.tianya.bigdata.homework.day20200812;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JobInfos implements Serializable {

    private String TaskName;

    private Integer accessTotals;

    private Integer accessFormats;

    private Integer accessErrors;

    private Integer runTime;

    private String day;

    private String startTime;

    private String endTime;


    public JobInfos() {
    }

    public Integer getAccessTotals() {
        return accessTotals;
    }

    public void setAccessTotals(Integer accessTotals) {
        this.accessTotals = accessTotals;
    }

    public Integer getAccessFormats() {
        return accessFormats;
    }

    public void setAccessFormats(Integer accessFormats) {
        this.accessFormats = accessFormats;
    }

    public Integer getAccessErrors() {
        return accessErrors;
    }

    public void setAccessErrors(Integer accessErrors) {
        this.accessErrors = accessErrors;
    }

    public Integer getRunTime() {
        return runTime;
    }

    public void setRunTime(Integer runTime) {
        this.runTime = runTime;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getTaskName() {
        return TaskName;
    }

    public void setTaskName(String taskName) {
        TaskName = taskName;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "JobInfos{" +
                "TaskName='" + TaskName + '\'' +
                ", accessTotals=" + accessTotals +
                ", accessFormats=" + accessFormats +
                ", accessErrors=" + accessErrors +
                ", runTime=" + runTime +
                ", day='" + day + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                '}';
    }
}
