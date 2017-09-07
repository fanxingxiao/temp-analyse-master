package com.cmx.bigdata.App.inverted_indexs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cmx.bigdata.App.inverted_indexs.MyKey;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, MyKey,NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyKey,NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split("\\s+");
		MyKey myKey = new MyKey();
		myKey.setWord(split[0]);
		myKey.setName(split[1]);
		myKey.setQuantity(Integer.parseInt(split[2]));
		context.write(myKey, NullWritable.get());
	}
}
