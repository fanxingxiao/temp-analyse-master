package com.cmx.bigdata.App.topN;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable,Text, Text, Text>{
	//2011	上海	38	-5	17	20	1	6
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.split("\\s+")[0];
		context.write(new Text(year), value);
	}
}
