package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DDL {
    public static void main(String[] args) throws IOException {
        //创建配置文件对象，并指定zookeeper的连接地址
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "node1:60000");
        /*
         * HBase的connection对象是一个重量级的对象，将来编写代码(Spark、Flink)的时候，
         * 避免重复创建，使用一个对象就OK，因为它是线程安全的
         * */
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //通过调用方法创建表
        create(admin);
        //关流
        admin.close();
        connection.close();
    }
    public static void create(Admin admin) throws IOException {
        //通过HTableDescriptor(表描述器)来实现我们表的参数设置，包括表名，列族等等
        //指定表名
        TableName tableName = TableName.valueOf("demo1");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //通过列簇描述器添加第一个列族，并指定版本数
        hTableDescriptor.addFamily(new HColumnDescriptor("C1").setVersions(1,3));
        //通过列簇描述器添加第二个列族，并指定版本数
        hTableDescriptor.addFamily(new HColumnDescriptor("C2").setVersions(1,3));
        //创建表
        boolean myuser = admin.tableExists(tableName);
        if(!myuser){
            admin.createTable(hTableDescriptor);
        }
    }
    public static void delete(Admin admin) throws IOException {
        //指定表名
        TableName tableName = TableName.valueOf("demo1");
        //判断表是否存在
        if (admin.tableExists(tableName)){
            //存在，先禁用
            admin.disableTable(tableName);
            //再删除
            admin.deleteTable(tableName);
        }
    }
}
