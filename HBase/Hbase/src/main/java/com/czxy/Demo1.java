package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class Demo1 {
    public static void main(String[] args) throws IOException {
        //创建配置文件对象，并指定zookeeper的连接地址
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node01,node02");
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "node01:60000");
        /*
         * HBase的connection对象是一个重量级的对象，将来编写代码(Spark、Flink)的时候，
         * 避免重复创建，使用一个对象就OK，因为它是线程安全的
         * */
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        addDatas(connection);
        //关流
        admin.close();
        connection.close();
    }
    /**
     * 创建表
     */

    public static void createTable(Admin admin) throws IOException {
        //通过HTableDescriptor(表描述器)来实现我们表的参数设置，包括表名，列族等等
        //指定表名
        TableName tableName = TableName.valueOf("demo1");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        //添加第一个列族，并指定版本数
        hTableDescriptor.addFamily(new HColumnDescriptor("C1").setVersions(1,3));
        //创建表
        boolean myuser = admin.tableExists(tableName);
        if(!myuser){
            admin.createTable(hTableDescriptor);
        }
    }
    /**
    * 删除表
    */
    public void delete(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (admin.tableExists(TableName.valueOf("WATER_BILL"))){
            //存在
            //先禁用
            admin.disableTable(TableName.valueOf("WATER_BILL"));
            //再删除
            admin.deleteTable(TableName.valueOf("WATER_BILL"));
        }
        //关流
        admin.close();
        connection.close();
    }
    /**
     * 插入数据
     */
    public static void  addDatas(Connection connection) throws IOException {
        //获取表
        Table WATER_BILL = connection.getTable(TableName.valueOf("demo1"));
        //创建put对象，并指定rowkey
        Put put = new Put("4944191".getBytes());
        //插入一行，该行只有一个列簇
        put.addColumn("C1".getBytes(),"NAME".getBytes(), Bytes.toBytes("登卫红"));
        //插入数据
        WATER_BILL.put(put);
        //关闭表对象
        /*
        * HTable是一个轻量级的对象，可以经常创建
        * HTable它是一个非线程安全的API，用完Table需要close，因为它是非线程安全的
        * */
        WATER_BILL.close();
    }

    /**
     *通过rowkey获取行数据
     */
    public void GetRowData(Connection connection) throws IOException {
        //2.获取HTable
        Table table = connection.getTable(TableName.valueOf("WATER_BILL"));
        //3.使用rowkey构建Get对象
        Get get = new Get(Bytes.toBytes("4944191"));
        //4.执行get请求
        Result result = table.get(get);
        //5.获取所有单元格
        //列出所有单元格
        List<Cell> cells = result.listCells();
        //6.打印rowkey
        byte[] rowkey = result.getRow();
        System.out.println("行键："+Bytes.toString(rowkey));

        //7.迭代单元格列表
        for (Cell cell : cells) {
            //获取列簇名称
            String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            //获取列名称
            String culumnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            //获取值
            String Value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(cf+":"+culumnName+" -> "+Value);
        }
        //关闭HTable
        table.close();
    }
    /**
     *通过rowkey删除行数据
     */
    public void DeleteRowData(Connection connection) throws IOException {
        //2.获取表对象
        TableName tableName = TableName.valueOf("WATER_BILL");
        Table table = connection.getTable(tableName);
        //3.根据rowkey构建delete对象
        Delete delete = new Delete(Bytes.toBytes("4944191"));
        //4.执行delete请求
        table.delete(delete);
        //5.关闭表
        table.close();
    }
}
