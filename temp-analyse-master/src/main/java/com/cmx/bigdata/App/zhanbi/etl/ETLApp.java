package com.cmx.bigdata.App.zhanbi.etl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * ETL任务:e:extract; t:transaction; l:load
 * 数据清洗,数据转换,数据补全
 */
/**
 * 作用：补全数据
 * 用户id	日期 时间	点击页面
 * 用户id	姓名	省份
 * 
 * 省份 用户id	日期 时间	点击页面
 *
 *
 *mr任务
 *no reduce
 *NullWritable
 *分布式缓存
 *计数器
 *etl
 *
 *输入是：click.txt ---- args[0]
 *缓存文件(映射文件)是：user.txt ----args[2]
 *输出路径:args[1]
 */
public class ETLApp {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("etl");
		job.setJarByClass(ETLApp.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(ETLMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//添加缓存文件到hadoop分布式缓存中
		job.addCacheFile(new Path(args[2]).toUri());
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}
