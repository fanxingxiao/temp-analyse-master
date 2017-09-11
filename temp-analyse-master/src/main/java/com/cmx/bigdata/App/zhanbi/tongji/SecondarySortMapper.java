package com.cmx.bigdata.App.zhanbi.tongji;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			String[] split = string.split("\\s+");
			String time=split[2].substring(0,2);
			context.write(new Text(time), new IntWritable(1));
		}
}
