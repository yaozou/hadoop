package com.yaozou.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.IOUtils;

public class HBaseTest {

	public static void main(String[] args) {

	}
	
	public static void testFilter() {
		HTable table = null;
		try {
			Scan scan = new Scan();
			table = getTable();
			Filter filter = new ColumnPrefixFilter(Bytes.toBytes("rk"));
			filter = new PageFilter(3l); //3
			
			// create HBase comparator
			ByteArrayComparable comparable = new SubstringComparator("lisi");
			filter = new SingleColumnValueFilter(
					Bytes.toBytes("info"), 
					Bytes.toBytes("name"), 
					CompareOp.EQUAL, 
					comparable);
			
			scan.setFilter(filter);
			
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				printResult(result);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally{
			if (table == null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void scanData() {
		HTable table = null;
		try {
			// get table
			table = getTable();
			// get scan
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("rk0001"));
			scan.setStopRow(Bytes.toBytes("rk0003"));
			
			//set cache
			scan.setCacheBlocks(true);
			scan.setBatch(2); // return column 2
			scan.setCaching(2); // Set the number of rows for caching that will be passed to scanners
			
			//set permission
			//scan.setACL(perms);
			
			// get result scanner
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				printResult(result);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if (table != null) {
				IOUtils.closeStream(table);
			}
		}
	}
	
	public static void printResult(Result result) {
		for (Cell cell : result.rawCells()) {
			System.out.println("---------------------------");
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
		}
	}

	public static HTable getTable() throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		HTable table = new HTable(configuration, Bytes.toBytes("t1"));
		return table;
	}

	public static void putData() throws IOException {
		// get table
		HTable table = getTable();

		// put data
		Put put = new Put(Bytes.toBytes("rk0001"));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));

		table.put(put);

		//close resource
		table.close();
	}

	public static void getData() throws Exception{
		// get table
		HTable table = getTable();

		// get data
		Get get = new Get(Bytes.toBytes("rk0001"));

		// result
		Result result = table.get(get);

		printResult(result);
	}

	public static void testNamespace() throws Exception {
		// create a default configuration
		Configuration configuration = HBaseConfiguration.create();		
		// create a HBaseAdmin
		HBaseAdmin admin = new HBaseAdmin(configuration);
		
		// create namespace
		NamespaceDescriptor namespace = NamespaceDescriptor.create("ns1").build();
		admin.createNamespace(namespace);
		
		// close resource
		admin.close();
	}

	public void createTable() throws Exception {
		// create a default configuration
		Configuration configuration = HBaseConfiguration.create();
		// create a HBaseAdmin
		HBaseAdmin admin = new HBaseAdmin(configuration);

		// if exists. delete table
		boolean b = admin.tableExists(Bytes.toBytes("t1"));
		if (b) {
			admin.disableTable(Bytes.toBytes("t1"));
			admin.deleteTable(Bytes.toBytes("t1"));
		}

		// create table
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("t1"));
		table.addFamily(new HColumnDescriptor(Bytes.toBytes("info")));
		admin.createTable(table);

		// close resource
		admin.close();
	} 
}
