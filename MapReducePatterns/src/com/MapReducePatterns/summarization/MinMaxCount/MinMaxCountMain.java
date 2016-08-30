package com.MapReducePatterns.summarization.MinMaxCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MinMaxCountMain {

	public static void main(String[] args) throws Exception{
		JobConf conf = new JobConf(MinMaxCountMain.class);
		conf.setJobName("MaxMinCountMain");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(MinMaxCountTuple.class);
		
		conf.setMapperClass(MinMaxCountMapper.class);
		//conf.setCombinerClass(IntSumReducer.class);
		conf.setReducerClass(MinMaxCountReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
