package com.cmx.bigdata.App.topNs2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<MyKey,Text,Text,NullWritable>{
	int topN;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		topN=context.getConfiguration().getInt("top.num",1);
	}
	
	//3  asdjkfsad
	//2  asdfsadf
	//1 asdfsdaf
	//1 asdfasdf
	
	@Override
	protected void reduce(MyKey arg0, Iterable<Text> arg1,Context arg2)
			throws IOException, InterruptedException {
		for(Text text:arg1){
			if(topN>0){
				arg2.write(text,NullWritable.get());
				topN--;
			}else {
				break;
			}
		}
	}
}
