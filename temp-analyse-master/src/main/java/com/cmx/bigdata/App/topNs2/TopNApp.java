package com.cmx.bigdata.App.topNs2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * 历史最高温度/全局最高温度
 * reduce？1个
 * 
 * 
 * 
 * 
 * 
 */
public class TopNApp {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser genericOptionsParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = genericOptionsParser.getRemainingArgs();

		// 构造Job对象
		Job job = Job.getInstance(conf,"TopNApp");
		job.setJarByClass(TopNApp.class);

		// 设置输入
		TextInputFormat.setInputPaths(job, new Path(remainingArgs[0]));// 路径
		job.setInputFormatClass(TextInputFormat.class);// 读取格式

		// 设置map
		job.setMapperClass(TopNMapper.class);// 设置map类
		job.setMapOutputKeyClass(MyKey.class);// 设置map输出key
		job.setMapOutputValueClass(Text.class);// 设置map输出value

		// 设置reduce
		job.setReducerClass(TopNReducer.class);// 设置reduce
		job.setOutputKeyClass(Text.class);// 设置reduce输出key
		job.setOutputValueClass(NullWritable.class);// 设置reduce输出value
		job.setNumReduceTasks(1);

		// 设置输出
		TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1] +"/"+ System.currentTimeMillis()));// 设置输出路径
		job.setOutputFormatClass(TextOutputFormat.class);// 写出格式

		// 提交任务
		// job.submit();
		// 提交任务，等待任务结束，打印统计信息
		// 正常结束返回true，非正常结束返回false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
