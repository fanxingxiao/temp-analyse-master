package com.cmx.bigdata.App.zhanbi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	@Override
	protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text,IntWritable>.Context context) throws IOException, InterruptedException{
		//key --- 偏移量,一般不太用
		//value --- 一行文本
		//context --- 上下文,通过他能将数据写出去
		String line = value.toString();//把一个text对象装成String对象
		String [] split = line.split("\\s+");//将一行文本切分成单词数组,包含数字
		for(String word:split){
				context.write(new Text(split[0]),new IntWritable(1));//将一个字符串变成Text类型,将一个int变成IntWritable类型
		}
	}
}
