package com.cmx.bigdata.App.inverted_indexs;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<MyKey,NullWritable,Text,NullWritable>{
	@Override
	protected void reduce(MyKey arg0, Iterable<NullWritable> arg1,Reducer<MyKey, NullWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		for(NullWritable nullWritable:arg1){
			arg2.write(new Text(arg0.toString()),NullWritable.get());
		}
	}
}
