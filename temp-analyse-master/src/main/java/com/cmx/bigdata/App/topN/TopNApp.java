package com.cmx.bigdata.App.topN;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * 2011 上海 38 -5 17 20 1 6 2011 云南 40 -17 17 26 0 10 2011 内蒙古 40 -44 5 30 1 12
 * 2011 北京 36 -16 13 21 2 10 2011 台湾 36 0 23 23 1 7 2011 吉林 37 -34 6 26 2 11
 * 2011 四川 43 -31 15 34 0 9
 * 
 * 
 * 
 * 输出 2011 云南 40 -17 17 26 0 10 2011 吉林 37 -34 6 26 2 11 2011 台湾 36 0 23 23 1 7
 * 
 * 1、二次排序（作业） 2、treeSet
 * 
 *
 * 
 */
public class TopNApp {
	public static void main(String[] args) throws Exception {
		// 构造Job对象
		Job job = Job.getInstance();

		// 设置任务名，任务类
		job.setJobName("TopNApp");
		job.setJarByClass(TopNApp.class);

		// 设置输入
		TextInputFormat.setInputPaths(job, new Path(args[0]));// 路径
		job.setInputFormatClass(TextInputFormat.class);// 读取格式

		// 设置map
		job.setMapperClass(TopNMapper.class);// 设置map类
		job.setMapOutputKeyClass(Text.class);// 设置map输出key
		job.setMapOutputValueClass(Text.class);// 设置map输出value

		// 设置reduce
		job.setReducerClass(TopNReducer.class);// 设置reduce
		job.setOutputKeyClass(Text.class);// 设置reduce输出key
		job.setOutputValueClass(NullWritable.class);// 设置reduce输出value
		job.setNumReduceTasks(3);

		// 设置输出
		TextOutputFormat.setOutputPath(job, new Path(args[1] +"/"+ System.currentTimeMillis()));// 设置输出路径
		job.setOutputFormatClass(TextOutputFormat.class);// 写出格式

		// 提交任务
		// job.submit();
		// 提交任务，等待任务结束，打印统计信息
		// 正常结束返回true，非正常结束返回false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
