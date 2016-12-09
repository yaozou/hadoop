package com.yaozou.hbase.on_mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tab2TabMapReduce extends Configured implements Tool {
	@Override
	public int run(String[] arg0) throws Exception {
		// create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		// set run class
		job.setJarByClass(this.getClass());
		
		// set mapper
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(
				"tb1", // input table
				scan, // scan instance
				TabMapper.class,  // set mapper class
				Text.class, // mapper output key
				Put.class, // mapper output value
				job // set job
				);
		TableMapReduceUtil.initTableReducerJob(
				"tab2", // output table
				TabReducer.class, // set reducer class
				job //set job
				);
		job.setNumReduceTasks(1); // 1 task at least
		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.err.println("error with job!!!!!!");
		}
		return 0;
	}
	
	// mapper class
	public static class TabMapper extends TableMapper<Text, Put>{
		Text rowkey = new Text();
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Put>.Context context)
				throws IOException, InterruptedException {
			// get key
			byte[] bytes = key.get();
			rowkey.set(Bytes.toString(bytes));
			// put 
			Put put = new Put(bytes);
			for (Cell cell : value.rawCells()) {
				//add cell
				if ("info".equals(CellUtil.cloneFamily(cell))) {
					if ("name".equals(CellUtil.cloneQualifier(cell))) {
						put.add(cell);
					}
				}
			}
			
		}
	}
	
	// reducer class
	public static class TabReducer extends TableReducer<Text, Put, ImmutableBytesWritable>{
		@Override
		protected void reduce(
				Text key,
				Iterable<Put> values,
				Reducer<Text, Put, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			for (Put put : values) {
				context.write(null, put);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// create configuration
		Configuration configuration = HBaseConfiguration.create();
		
		// submit job
		int job = ToolRunner.run(configuration, new Tab2TabMapReduce(), args);
		
		//exit
		System.exit(job);
	}

}
