package com.MapReducePatterns.started.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable Key, Text value,  OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
		String line = value.toString(); // Read new line
		line = line.replace(".", ""); // Clean text
		
		StringTokenizer tokenizer = new StringTokenizer(line); // Tokenize string line
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			output.collect(word,one);
		}
	}
}
