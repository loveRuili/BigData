package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

public class DML {
    public static void main(String[] args) throws IOException {
        //创建配置文件对象，并指定zookeeper的连接地址
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        //configuration.set("hbase.master", "node1:60000");
        /*
         * HBase的connection对象是一个重量级的对象，将来编写代码(Spark、Flink)的时候，
         * 避免重复创建，使用一个对象就OK，因为它是线程安全的
         * */
        Connection connection = ConnectionFactory.createConnection(configuration);
        //1.使用一个测试表进行DML操作
        TableName tableName = TableName.valueOf("czxy:t1");
        Table table = connection.getTable(tableName);
        Admin admin = connection.getAdmin();
        //2.调用方法实现DML操作
        // 2.1：调用Put方法插入数据
        //Put(table);

        // 2.2：调用DeleteRowData方法删除数据
        //DeleteRowData(table);

        // 2.3 调用Update方法更新数据
        //Update(table);

        // 2.4 调用Get方法查看数据
        Get(table);
        //3.关闭流
        table.close();
        admin.close();
        connection.close();

    }
    /*
    * 插入数据
    * */
    public static void Put(Table table) throws IOException {
        Put put = new Put("001".getBytes());
        put.addColumn("C1".getBytes(),"name".getBytes(), Bytes.toBytes("张荷"));
        put.addColumn("C1".getBytes(),"age".getBytes(), Bytes.toBytes("20"));
        //插入数据(可插入一条，也可插入多条puts)
        table.put(put);
    }

    /*删除数据*/
    public static void DeleteRowData(Table table) throws IOException {
        Delete delete = new Delete("002".getBytes());
        // delete.addFamily()删除整个的数列族据
        // delete.addColumn(Bytes.toBytes("列族名"),Bytes.toBytes("列名")); //只删除最新版本
        // delete.addColumns(Bytes.toBytes("basic"),Bytes.toBytes("name")); //删除所有版本
        //也可以删除多条deletes
        table.delete(delete);
    }

    /*删除整块表*/
    public static void DeleteTable(Admin admin,TableName tableName) throws IOException {
        //判断表是否存在
        if (admin.tableExists(tableName)){
            //存在，先禁用，再删除
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

    }
    /*更新数据*/
    public static void Update(Table table) throws IOException {
        Put put = new Put("001".getBytes());
        put.addColumn("C1".getBytes(),"age".getBytes(), Bytes.toBytes("21"));
        table.put(put);
    }

    /*查询数据*/
    public static void Get(Table table) throws IOException {
        Get get = new Get("001".getBytes());
        Result result = table.get(get);
        // 一个Result对象代表一行rowkey数据
        // 一个Cell代表一列数据
        for(Cell cell : result.rawCells()){
            //输出每一列Cell对象中的数据：20201001_888  column=basic:age, timestamp=1616029665232, value=20
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) //获取这一列的rowkey，转换为字符串类型
                            +"\t"+
                            Bytes.toString(CellUtil.cloneFamily(cell)) //获取这一列的列族，转换为字符串类型
                            +":"+
                            Bytes.toString(CellUtil.cloneQualifier(cell)) //获取这一列的名称，转换为字符串类型
                            +"\t"+
                            Bytes.toString(CellUtil.cloneValue(cell)) //获取这一列的值，转换为字符串类型
                            +"\t"+
                            cell.getTimestamp() //获取时间戳
            );
        }
    }
}
