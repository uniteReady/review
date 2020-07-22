package com.tianya.bigdata.hadoop.day20200711;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeptWritable implements Writable, DBWritable {

    private int deptno;

    private String dname;

    private String loc;


    public DeptWritable() {
    }

    public DeptWritable(int deptno, String dname, String loc) {
        this.deptno = deptno;
        this.dname = dname;
        this.loc = loc;
    }



    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(deptno);
        out.writeUTF(dname);
        out.writeUTF(loc);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.deptno=in.readInt();
        this.dname = in.readUTF();
        this.loc = in.readUTF();
    }

    /**
     * Sets the fields of the object in the {@link PreparedStatement}.
     *
     * @param statement the statement that the fields are put into.
     * @throws SQLException
     */
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,deptno);
        statement.setString(2,dname);
        statement.setString(3,loc);

    }

    /**
     * Reads the fields of the object from the {@link ResultSet}.
     *
     * @param resultSet the {@link ResultSet} to get the fields from.
     * @throws SQLException
     */
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.deptno = resultSet.getInt(1);
        this.dname = resultSet.getString(2);
        this.loc = resultSet.getString(3);
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

    public String getDname() {
        return dname;
    }

    public void setDname(String dname) {
        this.dname = dname;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    @Override
    public String toString() {
        return deptno+ "\t"+dname+ "\t"+loc;
    }
}
