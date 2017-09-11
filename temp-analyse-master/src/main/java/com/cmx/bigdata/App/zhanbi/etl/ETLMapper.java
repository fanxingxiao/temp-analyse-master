package com.cmx.bigdata.App.zhanbi.etl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 读取一行数据，补全完以后输出
 *
 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	//key是用户ID,value是省份
	private Map<String,String> places = new HashMap<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException{
		//获取第一个缓存文件的文件名
		String fileName = new Path(context.getCacheFiles()[0]).getName();
		//通过文件名像读取本地文件一样读取缓存文件中的内容
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String tmp = null;
		while((tmp=reader.readLine())!=null){
			String[] split = tmp.split("\\s+");
			places.put(split[0],split[2]);
		}
		reader.close();
	}
	@Override
	protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException{
		String string = value.toString();
		String[] split = string.split("\\s+",4);
		String user = split[0];
		String province = places.get(user);
		String result=province+"\t"+split[0]+"\t"+split[1]+"\t"+split[2]+"\t"+split[3];
		context.write(new Text(result), NullWritable.get());//nullwritable是单例模式
		context.getCounter("etl","map").increment(1);
	}
}
