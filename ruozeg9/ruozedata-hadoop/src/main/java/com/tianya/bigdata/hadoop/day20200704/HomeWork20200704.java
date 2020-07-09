package com.tianya.bigdata.hadoop.day20200704;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;

public class HomeWork20200704 {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    //本次作业工作目录
    private static final String HDFS_BASE_PATH = "/ruozedata/hdfs-works";
    //HDFS的uri
    private static final String HDFS_URI = "hdfs://hadoop:9000";
    //操作HDFS的用户
    private static final String HDFS_USER = "hadoop";

    /**
     * 作业一
     *
     * @param day 入参，日期 yyyyMMdd
     */
    public static void rename(String day) {
        Configuration conf = null;
        FileSystem fileSystem = null;
        try {
            //生成FileSystem对象
            conf = new Configuration();
            URI uri = new URI(HDFS_URI);
            fileSystem = FileSystem.get(uri, conf, HDFS_USER);
            //根据传入的日期生成相应日期的HDFS路径
            Path srcPath = new Path(HDFS_BASE_PATH + File.separator + day);
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(srcPath, true);
            Path copySrcPath;
            Path copyDstPath;
            while (remoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = remoteIterator.next();
                Path path = fileStatus.getPath();
                //获取原文件路径
                String fileSrcPath = path.toString().substring(path.toString().indexOf(HDFS_BASE_PATH));
                copySrcPath = new Path(fileSrcPath);
                //生成新文件路径
                String fileName = path.getName();
                String prefixFileName = fileName.substring(0, fileName.indexOf("."));
                String newPrefixFileName = String.valueOf(Integer.valueOf(prefixFileName) - 1);
                String suffixFileName = fileName.substring(fileName.indexOf("."));
                String fileDstPath = HDFS_BASE_PATH + File.separator + day + "-" + newPrefixFileName + suffixFileName;
                copyDstPath = new Path(fileDstPath);
                //rename成目标文件
                String renameStatus = fileSystem.rename(copySrcPath, copyDstPath) ? "成功" : "失败";
                LOGGER.info("原文件:" + copySrcPath.toString() + "重命名为:" + copyDstPath.toString() + renameStatus);
            }
            //删除掉原来的空目录
            String deleteStatus = fileSystem.delete(srcPath, true) ? "成功" : "失败";
            LOGGER.info("删除原目录:" + srcPath.toString() + deleteStatus);

        } catch (URISyntaxException | IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (null != fileSystem) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    fileSystem = null;
                }
            }
        }

    }

    /**
     * 作业二
     *
     * @throws Exception
     */
    public static void merge() {
        //读取2个文件 获取输入流
        FileInputStream filePartIn0 = null;
        FileInputStream filePartIn1 = null;
        FileOutputStream out = null;
        try {
            //获取输出流
            filePartIn0 = new FileInputStream(new File("out/hadoop.tgz.part0"));
            filePartIn1 = new FileInputStream(new File("out/hadoop.tgz.part1"));
            out = new FileOutputStream(new File("out/hadoop.tar.gz"));
            //拷贝
            IOUtils.copyBytes(filePartIn0, out, 4096);
            IOUtils.copyBytes(filePartIn1, out, 4096);
            LOGGER.info("拷贝完成");
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            //关流
            IOUtils.closeStream(filePartIn0);
            IOUtils.closeStream(filePartIn1);
            IOUtils.closeStream(out);
        }
    }

    /**
     * 下载block1
     */
    public static void download1() {
        Configuration conf = null;
        FileSystem fileSystem = null;
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
            //生成FileSystem对象
            conf = new Configuration();
            URI uri = new URI(HDFS_URI);
            fileSystem = FileSystem.get(uri, conf, HDFS_USER);
            in = fileSystem.open(new Path("/hdfsapi/pk/hadoop-2.6.0.tar.gz"));
            out = new FileOutputStream(new File("out/hadoop.tgz.part0"));
            byte[] buffer = new byte[2048];
            for (int i = 0; i < 1024 * 1024 * 128; i += buffer.length) {
                in.read(buffer);
                out.write(buffer);
            }
        } catch (URISyntaxException | IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }


    /**
     * 下载block2
     */
    public static void download2(){
        Configuration conf = null;
        FileSystem fileSystem = null;
        FSDataInputStream in = null;
        FileOutputStream out = null;
        try {
            //生成FileSystem对象
            conf = new Configuration();
            URI uri = new URI(HDFS_URI);
            fileSystem = FileSystem.get(uri, conf, HDFS_USER);
            in = fileSystem.open(new Path("/hdfsapi/pk/hadoop-2.6.0.tar.gz"));
            out = new FileOutputStream(new File("out/hadoop.tgz.part1"));
            in.seek(1024 * 1024 * 128);
            IOUtils.copyBytes(in, out, fileSystem.getConf());
        } catch (URISyntaxException | IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    public static void main(String[] args) {
        rename("20211001");
        merge();
    }


}
