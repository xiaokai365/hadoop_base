package com.snail.mr.wortcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WRRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job  wcjob=Job.getInstance(conf);
		//相当于设置classpath  以便能找到WRRunner依赖的2个类  map   reduce
		wcjob.setJarByClass(WRRunner.class);
		
		wcjob.setMapperClass(WRMapper.class);
		wcjob.setReducerClass(WRReduce.class);
		//设置map的输出key类型以及输出value类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		//设置reduce的输出key类型以及输出value类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(wcjob, new Path("/data"));
		FileOutputFormat.setOutputPath(wcjob, new Path("/data/output"));
		//true显示出map  reduce进度
		int result=wcjob.waitForCompletion(true)?0:1;
		System.exit(result);//0为正常退出  1为异常退出
		
		
	}
}
