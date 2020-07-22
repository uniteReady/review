package com.tianya.bigdata.homework.day20200719;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Delete {

    public static final String FILE_NAME = "_remote.repositories";

    public static final String TARGET_DIR = "D:\\maven\\mvn_repo";
//    public static final String TARGET_DIR = "D:\\temp_test";

    public static List<File> getAllFiles(File targetDir,List<File> filesList){
        if(targetDir.isDirectory()){
            File[] files = targetDir.listFiles();
            for (File file : files) {
                getAllFiles(file,filesList);
            }

        }else{
           if(FILE_NAME.equals(targetDir.getName())){
               filesList.add(targetDir);
           }
//            filesList.add(targetDir);
        }
        return filesList;

    }

    public static void main(String[] args) {
        File targetDir = new File(TARGET_DIR);

        List<File> filesList = new ArrayList<>();
        filesList = getAllFiles(targetDir,filesList);
        int count = 0;
        for (File file : filesList) {
            file.delete();
            count++;
        }

        System.out.println(count);

    }
}
