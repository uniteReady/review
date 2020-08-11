package com.ruozedata.bigdata.utils;

import com.ruozedata.bigdata.domain.Access;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MockData {

    public String URL = "http://hadoop:6789/log/upload";

    @Test
    public void  testUpload() throws InterruptedException {

        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

        for (int i = 0; i < 10; i++) {
            Access access = new Access();
            access.setId(i);
            access.setName("PK"+ i);
            access.setTime(format.format(new Date()));
            UploadUtils.upload(URL,access.toString());
//            System.out.println(access);
            Thread.sleep(2000);

        }

    }
}
