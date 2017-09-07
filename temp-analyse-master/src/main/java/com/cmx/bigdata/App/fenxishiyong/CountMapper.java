package com.cmx.bigdata.App.fenxishiyong;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable,Text,MyKey,MyValue>{
	private String place;
	private String date;
	@Override
	protected void setup(Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		place = conf.get("count.place","province");
		date = conf.get("count.date","year");
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyKey, MyValue>.Context context)
			throws IOException, InterruptedException {
		//上海    上海    2011-01-01      6       2       4       4       多云~雨夹雪 北风 4-5级
		String line = value.toString();
		//上海    上海     2011-01	     6       2       4       4
		String[] split = line.split("[\\s]+");
		//构造mykey
		MyKey myKey=new MyKey();
		if("province".equals(place)){
			myKey.setCity(split[0]);
		}else{
			myKey.setCity(split[0]+" "+split[1]);
		}
		String[] split2=split[2].split("-");
		if("year".equals(date)){
			myKey.setMonth(split2[0]);
		}else{
			myKey.setMonth(split2[0]+" "+split2[1]);
		}
		//构造myvalue
		MyValue myValue = new MyValue();
		myValue.setMax(Integer.parseInt(split[3]));
		myValue.setDiff(Integer.parseInt(split[5]));
		myValue.setMin(Integer.parseInt(split[4]));
		myValue.setAvg(Integer.parseInt(split[6]));
		context.write(myKey, myValue);
		context.getCounter("count_by_"+place+"_"+date, "mapper").increment(1);
	}
}
