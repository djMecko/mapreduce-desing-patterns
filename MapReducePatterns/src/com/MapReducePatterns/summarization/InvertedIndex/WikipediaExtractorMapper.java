package com.MapReducePatterns.summarization.InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WikipediaExtractorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	private Text link = new Text();
	private Text id = new Text();
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line); // Tokenize string line
		
		int count = 0;
		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();
			if(count == 0){
				link.set(token);
			}else if(count == 1){
				id.set(token);
			}
			System.out.println(token);
			count++;
		}
		output.collect(link,id);
	}
	
}
