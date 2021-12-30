package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class ScanAndFilter {
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

        //2.根据需求调用方法完成对应操作
        // 2.1：查询2021年1月和2月的数据
        demo1(table);
        // 2.2：查询2021年的所有数据
        // 2.3：查询所有age = 20的数据
        // 2.4：查询所有数据的name和age这两列
        // 2.5：查询所有age = 20的人的name和age
        //3.关闭流
        table.close();
        connection.close();
    }
    /*查询2021年1月和2月的数据*/
    //前缀范围过滤器
    public static void demo1(Table table) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("202101"));
        scan.withStopRow(Bytes.toBytes("202103"));
        //执行scan：返回值是ResultScanner，包含了多个Rowkey的数据
        ResultScanner rsScan = table.getScanner(scan);
        /**
         *      ResultScanner：包含多个Rowkey的数据，包含了多个Result对象：Iterator<Result>
         *      Result：一个Rowkey的数据，包含了这个Rowkey多列的数据：Cell[]
         *      Cell ：一列的数据
         */
        for (Result result : rsScan) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell)) //获取这一列的rowkey，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell)) //获取这一列的列族，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell)) //获取这一列的名称，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell)) //获取这一列的值，转换为字符串类型
                                +"\t"+
                                cell.getTimestamp() //获取时间戳
                );
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }
    /*查询2021年的所有数据*/
    //前缀过滤器
    public static void demo2(Table table) throws IOException {
        Scan scan = new Scan();
        Filter prefixFiter = new PrefixFilter(Bytes.toBytes("2021"));
        scan.setFilter(prefixFiter);
        ResultScanner rsScan = table.getScanner(scan);
        for (Result result : rsScan) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell)) //获取这一列的rowkey，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell)) //获取这一列的列族，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell)) //获取这一列的名称，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell)) //获取这一列的值，转换为字符串类型
                                +"\t"+
                                cell.getTimestamp() //获取时间戳
                );
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }

    /*查询所有age = 20的数据*/
    //列值过滤器
    public static void demo3(Table table) throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                //指定列簇
                Bytes.toBytes("C1"),
                Bytes.toBytes("age"),//指定列
                CompareOperator.EQUAL,//指定比较器类型
                Bytes.toBytes("20")  //比较的值
        );
        scan.setFilter(singleColumnValueFilter);
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell)) //获取这一列的rowkey，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell)) //获取这一列的列族，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell)) //获取这一列的名称，转换为字符串类型
                                +"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell)) //获取这一列的值，转换为字符串类型
                                +"\t"+
                                cell.getTimestamp() //获取时间戳
                );
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }
    /*查询所有数据的name和age这两列*/
    //列过滤器
    public static void demo4(Table table) throws IOException {
        Scan scan = new Scan();
        byte[][] prefixes = {
                Bytes.toBytes("name"),
                Bytes.toBytes("age")
        };
        Filter columnFilter = new MultipleColumnPrefixFilter(prefixes);
        scan.setFilter(columnFilter);
        ResultScanner sc = table.getScanner(scan);
        for (Result result : sc) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                          CellUtil.cloneRow(cell)+"\t"+
                          CellUtil.cloneFamily(cell)+"\t"+
                          CellUtil.cloneQualifier(cell)+"\t"+
                          CellUtil.cloneValue(cell)+"\t"+
                          cell.getTimestamp()
                );
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }
    /*查询所有age = 20的人的name和age*/
    //组合过滤器
    public static void demo5(Table table) throws IOException {
        Scan scan = new Scan();
        //年龄过滤
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("C1"),//指定列簇
                Bytes.toBytes("age"),//指定列
                CompareOperator.EQUAL,//指定比较器类型
                Bytes.toBytes("20")  //比较的值
        );

        //字段过滤
        byte[][] prefixes = {
                Bytes.toBytes("name"),
                Bytes.toBytes("age")
        };
        Filter columnFilter = new MultipleColumnPrefixFilter(prefixes);
        FilterList filters = new FilterList();
        filters.addFilter(singleColumnValueFilter);
        filters.addFilter(columnFilter);
        scan.setFilter(filters);
        ResultScanner sc = table.getScanner(scan);
        for (Result result : sc) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        CellUtil.cloneRow(cell)+"\t"+
                                CellUtil.cloneFamily(cell)+"\t"+
                                CellUtil.cloneQualifier(cell)+"\t"+
                                CellUtil.cloneValue(cell)+"\t"+
                                CellUtil.cloneValue(cell)+"\t"+
                                cell.getTimestamp()
                );
            }
            System.out.println("----------------------------------------------------------------------");
        }
    }
}
