package com.snail.mr.wortcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WRReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		long temp=0;
		for (LongWritable value : values) {
				temp+=value.get();
		}
		
		context.write(new Text(key), new LongWritable(temp));
		
	}

	 
}
