package com.cmx.bigdata.App.zhanbi.tongji;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<Text,IntWritable, Text,IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable intWritable:arg1){
			//IntWritable类型转换成 int类型对象
			int i = intWritable.get();
			sum+=i;
		}
		//int 类型转换成intWritable对象
		arg2.write(arg0, new IntWritable(sum));
		//reduce 计算传输数据的数量
		arg2.getCounter("secondary_sort","reduce").increment(1);
	}
}
