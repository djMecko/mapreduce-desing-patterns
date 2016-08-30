package com.MapReducePatterns.summarization.MinMaxCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MinMaxCountMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, MinMaxCountTuple>{
	
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); 
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, MinMaxCountTuple> output, Reporter reporter) throws IOException {
		String line = value.toString(); // Read new line
		StringTokenizer tokenizer = new StringTokenizer(line); // Tokenize string line
		
		int count = 0;
		while(tokenizer.hasMoreTokens()){
			if(count == 0){
				outUserId.set(tokenizer.nextToken());
			}else if(count == 1){
				Date creationDate = new Date();
				try {
					creationDate = frmt.parse(tokenizer.nextToken());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				outTuple.setMax(creationDate);
				outTuple.setMin(creationDate);
			}
			count++;
		}
		outTuple.setCount(1);
		output.collect(outUserId,outTuple);
	}

}
