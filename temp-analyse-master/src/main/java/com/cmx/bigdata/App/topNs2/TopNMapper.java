package com.cmx.bigdata.App.topNs2;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable,Text, MyKey, Text>{
	//2011	上海	38	-5	17	20	1	6
	int topN;
	TreeSet<String> set;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, MyKey, Text>.Context context)
			throws IOException, InterruptedException {
		topN=context.getConfiguration().getInt("top.num",1);
		set=new TreeSet<>((x,y)->{
			return getMax(y)-getMax(x);
		});
	}
	
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(set.size()<topN)
			set.add(line);
		else{
			String last = set.last();
			if(getMax(line)>getMax(last)){
				set.pollLast();
				set.add(line);
			}
		}
		//38 , 2011	上海	38	-5	17	20	1	6
//		context.write(new MyKey(max), value);
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, MyKey, Text>.Context context)
			throws IOException, InterruptedException {
		for(String string:set){
			int max = getMax(string);
			context.write(new MyKey(max),new Text(string));
		}
		
		
	}
	
	private int getMax(String string){
		return Integer.parseInt(string.split("\\s+")[2]);
	}
	
	
	
}
