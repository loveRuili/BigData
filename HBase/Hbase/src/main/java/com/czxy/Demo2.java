package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class Demo2 {
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
        Table table = connection.getTable(TableName.valueOf("czxy:demo2"));

        //创建这张表
        //create(admin);

        //向表中添加数据
        Put(table);

        //删除一条数据
        //delete(table);
    }

    private static void delete(Table table) throws IOException {
        Delete delete = new Delete("001".getBytes());
        table.delete(delete);
        System.out.println("删除成功！");
    }

    private static void Put(Table table) throws IOException {
        //一个Put表示一条数据
        Put put = new Put("005".getBytes());
        put.addColumn("C1".getBytes(),"name".getBytes(),"赵六".getBytes());
        put.addColumn("C1".getBytes(),"age".getBytes(),"1".getBytes());
        table.put(put);
        System.out.println("put成功");
    }

    private static void create(Admin admin) throws IOException {
        //指定表
        TableName tableName = TableName.valueOf("czxy:demo2");
        //判断表是否存在
        if (!admin.tableExists(tableName)){
            //不存在，则先创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //给该表添加列簇，并指定版本数
            hTableDescriptor.addFamily(new HColumnDescriptor("C1").setVersions(1,2));
            admin.createTable(hTableDescriptor);
            System.out.println("创建成功！");
        }
    }



}
