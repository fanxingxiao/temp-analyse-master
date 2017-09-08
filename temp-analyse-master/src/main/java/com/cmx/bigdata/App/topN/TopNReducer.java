package com.cmx.bigdata.App.topN;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<Text,Text,Text,NullWritable>{
	//年份一样的数据进入同一个reduce方法
	//前3
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		//自定义排序规则
		//2011	上海	38	-5	17	20	1	6
		TreeSet<String> set=new TreeSet<>((x,y)->{
			int i = getMax(y)-getMax(x);
			return i==0?1:i;
		});
		//取前3
		for(Text text:arg1){
			if(set.size()<3)
				set.add(text.toString());
			else {
				if(getMax(text.toString())>getMax(set.last())){
					set.pollLast();
					set.add(text.toString());
				}
			}
		}
		for(String text:set){
			arg2.write(new Text(text), NullWritable.get());
		}
	}
	private int getMax(String text){
		return Integer.parseInt(text.split("\\s+")[2]);
	}
	
}
