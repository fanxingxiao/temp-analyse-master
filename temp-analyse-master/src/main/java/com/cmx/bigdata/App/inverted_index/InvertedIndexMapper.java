package com.cmx.bigdata.App.inverted_index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//获取文件切片
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		//获取文件名
		String fileName = fileSplit.getPath().getName();
		String line = value.toString();
		String[] split = line.split("\\W+");
		for (String word : split) {
			if (!"".equals(word)) {
				context.write(new Text(word+"\t"+fileName), new IntWritable(1));
			}
		}
	}
}
