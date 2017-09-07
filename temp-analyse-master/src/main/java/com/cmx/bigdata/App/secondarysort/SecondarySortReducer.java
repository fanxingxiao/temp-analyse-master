package com.cmx.bigdata.App.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<MyKey,NullWritable, Text,NullWritable>{
	@Override
	protected void reduce(MyKey arg0, Iterable<NullWritable> arg1,
			Reducer<MyKey, NullWritable, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		/*
		2011	安徽	41	-16	16	24	1	8
		2011	山东	37	-22	13	21	1	9
		2011	山西	39	-29	10	25	-2	12
		2011	广东	38	-26	22	20	1	7
		2011	广西	39	-16	20	23	0	7
		2011	新疆	47	-43	9	30	0	12
		2011	江苏	38	-12	15	24	1	8
		2011	江西	39	-16	18	20	0	8
		2011	河北	38	-38	12	27	1	10
		2011	河南	42	-28	15	22	0	9
		2011	海南	38	7	24	18	1	7
		2011	浙江	40	-6	17	20	1	8
		2011	湖北	40	-16	17	24	0	8
		2011	湖南	40	-24	17	24	0	7
		2011	澳门	35	0	22	27	2	6
		2011	甘肃	40	-29	9	29	1	12
		2011	福建	39	-36	19	24	1	8
		2011	西藏	39	-32	6	28	1	13
		2011	贵州	43	-8	16	22	-2	7
		2011	辽宁	36	-31	9	24	1	10
		2011	重庆	42	-27	17	24	0	7
		2011	陕西	40	-23	13	26	1	10
		2011	青海	35	-32	4	30	2	14
		2011	香港	36	6	23	10	2	4
		2011	黑龙江	39	-44	4	29	1	11
		*/
		
		/*2011	浙江	40	-6	17	20	1	8
		2011	湖北	40	-16	17	24	0	8
		2011	湖南	40	-24	17	24	0	7*/
		for(NullWritable nullWritable:arg1){
			arg2.write(new Text(arg0.toString()),NullWritable.get());
		}
	}
}
