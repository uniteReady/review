package com.tianya.bigdata.homework.day20200812;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JobInfos implements Writable, DBWritable {

    private Integer accessTotals;

    private Integer accessFormats;

    private Integer accessErrors;

    private Integer runTime;

    private String day;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(accessTotals);
        out.writeInt(accessFormats);
        out.writeInt(accessErrors);
        out.writeInt(runTime);
        out.writeUTF(day);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.accessTotals = in.readInt();
        this.accessFormats = in.readInt();
        this.accessErrors = in.readInt();
        this.runTime = in.readInt();
        this.day = in.readUTF();

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,this.accessTotals);
        statement.setInt(2,this.accessFormats);
        statement.setInt(3,this.accessErrors);
        statement.setInt(4,this.runTime);
        statement.setString(5,this.day);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.accessTotals = resultSet.getInt(1);
        this.accessFormats = resultSet.getInt(2);
        this.accessErrors = resultSet.getInt(3);
        this.runTime = resultSet.getInt(4);
        this.day = resultSet.getString(5);

    }

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

    @Override
    public String toString() {
        return "JobInfos{" +
                "accessTotals=" + accessTotals +
                ", accessFormats=" + accessFormats +
                ", accessErrors=" + accessErrors +
                ", runTime=" + runTime +
                ", day='" + day + '\'' +
                '}';
    }
}
