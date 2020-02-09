package cn.spark.study.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool {
    //静态的Connection队列
    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //获取连接
    public  synchronized  static  Connection getConnection(){
        try{

            if(connectionQueue == null){
                connectionQueue = new LinkedList<>();
                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","root");
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e){

        }
        return connectionQueue.poll();
    }

    //返还连接
    public static void returnConnection(Connection connection){
        connectionQueue.push(connection);
    }

}
