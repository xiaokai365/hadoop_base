package com.snail.mr.wortcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * wordcount map
 * @author snail
 *
 */
public class WRMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
			
		String line=value.toString();
		String[] contents=line.split("\t");
		for(String con:contents){
			context.write(new Text(con), new LongWritable(1));
		}
		
	}
	
}
