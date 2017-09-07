package com.cmx.bigdata.App.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable,Text,MyKey,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyKey, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			String[] split = string.split("\\s+");
			MyKey myKey=new MyKey();
			System.out.println(split[0]);
			myKey.setDate(Integer.parseInt(split[0]));
			myKey.setPlace(split[1]);
			myKey.setMax(Integer.parseInt(split[2]));
			myKey.setMin(Integer.parseInt(split[3]));
			myKey.setAvg(Integer.parseInt(split[4]));
			myKey.setMax_diff(Integer.parseInt(split[5]));
			myKey.setMin_diff(Integer.parseInt(split[6]));
			myKey.setAvg_diff(Integer.parseInt(split[7]));
			context.write(myKey, NullWritable.get());
		}

}
