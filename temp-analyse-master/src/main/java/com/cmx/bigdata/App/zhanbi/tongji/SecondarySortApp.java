package com.cmx.bigdata.App.zhanbi.tongji;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class SecondarySortApp {
	public static void main(String[] args) throws Exception {
		// 构造Job对象
		Job job = Job.getInstance();

		// 设置任务名，任务类
		job.setJobName("secondary_sort");
		job.setJarByClass(SecondarySortApp.class);

		// 设置输入
		TextInputFormat.setInputPaths(job, new Path("E:/test/kaoshi/in/click.txt"));// 路径
		job.setInputFormatClass(TextInputFormat.class);// 读取格式

		// 设置map
		job.setMapperClass(SecondarySortMapper.class);// 设置map类
		job.setMapOutputKeyClass(Text.class);// 设置map输出key
		job.setMapOutputValueClass(IntWritable.class);// 设置map输出value

		// 设置reduce
		job.setReducerClass(SecondarySortReducer.class);// 设置reduce
		job.setOutputKeyClass(Text.class);// 设置reduce输出key
		job.setOutputValueClass(IntWritable.class);// 设置reduce输出value
		job.setNumReduceTasks(1);

		// 设置输出
		TextOutputFormat.setOutputPath(job, new Path("E:/test/kaoshi/out"+"/" + System.currentTimeMillis()));// 设置输出路径
		job.setOutputFormatClass(TextOutputFormat.class);// 写出格式

		// 提交任务
		// job.submit();
		// 提交任务，等待任务结束，打印统计信息
		// 正常结束返回true，非正常结束返回false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
