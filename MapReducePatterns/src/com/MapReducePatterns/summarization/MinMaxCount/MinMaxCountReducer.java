package com.MapReducePatterns.summarization.MinMaxCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MinMaxCountReducer extends MapReduceBase implements Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>{

	private  MinMaxCountTuple result = new MinMaxCountTuple();
	
	@Override
	public void reduce(Text key, Iterator<MinMaxCountTuple> values, OutputCollector<Text, MinMaxCountTuple> output, Reporter reporter) throws IOException {
		
		result.setMax(null);
		result.setMin(null);
		result.setCount(0);
		
		int sum = 0;
		
		while(values.hasNext()){
			MinMaxCountTuple next = values.next();
			if(result.getMin() == null || next.getMin().compareTo(result.getMin()) < 0  ){
				result.setMin(next.getMin());
			}
			if(result.getMax() == null || next.getMax().compareTo(result.getMax()) > 0){
				result.setMax(next.getMax());
			}
			sum += next.getCount();
		}
		result.setCount(sum);
		output.collect(key, result);
	}

}
