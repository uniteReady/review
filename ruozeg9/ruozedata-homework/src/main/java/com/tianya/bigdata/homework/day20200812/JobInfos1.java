package com.tianya.bigdata.homework.day20200812;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JobInfos1 implements Writable, DBWritable {

    private String TaskName;

    private Integer accessTotals;

    private Integer accessFormats;

    private Integer accessErrors;

    private Integer runTime;

    private String day;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(TaskName);
        out.writeInt(accessTotals);
        out.writeInt(accessFormats);
        out.writeInt(accessErrors);
        out.writeInt(runTime);
        out.writeUTF(day);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.TaskName = in.readUTF();
        this.accessTotals = in.readInt();
        this.accessFormats = in.readInt();
        this.accessErrors = in.readInt();
        this.runTime = in.readInt();
        this.day = in.readUTF();

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,this.TaskName);
        statement.setInt(2,this.accessTotals);
        statement.setInt(3,this.accessFormats);
        statement.setInt(4,this.accessErrors);
        statement.setInt(5,this.runTime);
        statement.setString(6,this.day);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.TaskName = resultSet.getString(1);
        this.accessTotals = resultSet.getInt(2);
        this.accessFormats = resultSet.getInt(3);
        this.accessErrors = resultSet.getInt(4);
        this.runTime = resultSet.getInt(5);
        this.day = resultSet.getString(6);

    }

    public JobInfos1() {
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


    @Override
    public String toString() {
        return TaskName + '\t' +
                accessTotals + '\t' +
                accessFormats + '\t' +
                accessErrors + '\t' +
                runTime + '\t' +
                day ;
    }
}
