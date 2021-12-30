package com.czxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Scanner;

public class demo3_ScanFilter {
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
        //前缀范围过滤器(将rowkey为001-100的数据过滤出来)
        //withStart(table);

        //前缀过滤器(将rowkey为001-009的数据过滤出来)
        //prefexFilter(table);

        //列过滤器(只查询name这一列)
        //ColumnFilter(table);

        //列值过滤器
        ColumnValueFilter(table);

    }

    private static void ColumnValueFilter(Table table) throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
            Bytes.toBytes("C1"),
                Bytes.toBytes("name"),
                CompareOperator.EQUAL,
                Bytes.toBytes("李四")
        );
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                                cell.getTimestamp()
                );
            }
            System.out.println("----------------------");
        }
    }

    private static void ColumnFilter(Table table) throws IOException {
        Scan scan = new Scan();
        byte[][] prefixes={
                Bytes.toBytes("name")
        };
        MultipleColumnPrefixFilter columnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
        scan.setFilter(columnPrefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                                cell.getTimestamp()
                );
            }
            System.out.println("----------------------");
        }
    }

    private static void prefexFilter(Table table) throws IOException {
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        scan.setFilter(prefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                                cell.getTimestamp()
                );
            }
            System.out.println("----------------------");
        }
    }

    private static void withStart(Table table) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow("001".getBytes());
        scan.withStopRow("100".getBytes());

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.listCells()) {
                System.out.println(
                        //获取rowkey
                        Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                                Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                                cell.getTimestamp()
                );
            }

            System.out.println("-------------------------------------------------------------------");
        }
    }
}
