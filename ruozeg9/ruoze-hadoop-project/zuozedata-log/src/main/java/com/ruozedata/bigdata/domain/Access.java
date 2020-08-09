package com.ruozedata.bigdata.domain;

import java.io.Serializable;

public class Access implements Serializable {

    private int id;

    private String name;

    private String time;


    public Access() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return id + "\t" + name + "\t" + time;
    }
}
