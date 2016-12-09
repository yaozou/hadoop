package com.yaozou.hbase.on_mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2TabMapReduce extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		//create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
		
		// set class
		job.setJarByClass(this.getClass());
		
		// set path
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		
		// set mapper
		job.setMapperClass(HDFS2TableMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		// set reduce
		TableMapReduceUtil.initTableReducerJob(
				"user", // set table
				null, 
				job
				);
		job.setNumReduceTasks(0);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.err.println("with job error");
		}
		
		return 0;
	}
	
	public static class HDFS2TableMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\t");
			//rk0001 zhangsan 33
			Put put = new Put(Bytes.toBytes(words[0]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(words[2]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(words[3]));
			
			rowkey.set(Bytes.toBytes(words[0]));
			context.write(rowkey, put);
		}
	}

	public static void main(String[] args) throws Exception {
		// get configuration
		Configuration configuration = HBaseConfiguration.create();
		// submit job
		int status =  ToolRunner.run(configuration, new HDFS2TabMapReduce(), args);
		//exit
		System.exit(status);
	}
}
