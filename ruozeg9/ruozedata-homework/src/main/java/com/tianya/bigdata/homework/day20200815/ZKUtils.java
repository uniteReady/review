package com.tianya.bigdata.homework.day20200815;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.StringJoiner;


public class ZKUtils {

    /**
     * 递归删除节点
     * @param path
     * @param zk
     * @return
     * @throws Exception
     */
    public static boolean deleteNodeRecursion(String path,ZooKeeper zk) throws Exception{
        if(null != path && exists(path,zk) && path.contains("/") && !"/".equals(path.trim())){
            delete(path, zk);
            path = path.substring(0,path.lastIndexOf("/"));
            System.out.println(path);
            if(getChildren(path,zk).size()!=0){
                //如果父目录下还有其他目录，则父目录不能被删除
                return false;
            }
            //递归去删除
            deleteNodeRecursion(path,zk);
            return true;
        }
        return false;
    }


    /**
     * 级联创建 /a/b/c
     * @param path
     * @param value
     * @param zk
     * @return
     * @throws Exception
     */
    public static boolean createNodeCascade(String path,String value,ZooKeeper zk) throws Exception{
        if(exists(path,zk)){
            //目录已经存在，直接返回
            return true;
        }
        if(null != path && path.contains("/")){
            String lastPath = "";
            String[] paths = path.split("/");
            for (int i = 0; i < paths.length; i++) {
                if (i == 0) {
                    continue;
                }
                if(null != paths[i] && !"".equals(paths[i].trim())){
                    lastPath +="/" + paths[i].trim() ;
                    if(!exists(lastPath,zk)){
                        if(i == paths.length -1 ){
                            createNode(lastPath, value, zk);
                        }else {
                            createNode(lastPath, "", zk);
                        }
                    }
                }
            }
            return true;
        }
        return false;
    }

    public static ZooKeeper getZK(String connect,int timeout) throws Exception {
        return new ZooKeeper(connect,timeout,null);
    }

    public static String createNode(String path,String value,ZooKeeper zk) throws Exception {
        return zk.create(path,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static String getData(String path ,ZooKeeper zk) throws Exception{
        return new String(zk.getData(path,false,null));
    }

    public static boolean updateData(String path,String value,ZooKeeper zk) throws Exception{
        zk.setData(path,value.getBytes(),-1);
        String data = getData(path, zk);
        return value.equals(data);
    }

    public static boolean exists(String path,ZooKeeper zk) throws Exception{
        return zk.exists(path,false)!=null;
    }

    public static List<String> getChildren(String path,ZooKeeper zk) throws Exception{
        return zk.getChildren(path,false);
    }

    public static void delete(String path,ZooKeeper zk) throws Exception{
        zk.delete(path,-1);
    }


}
