package com.cmx.bigdata.App.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;


/**
 * 自定义类 1、写一个类，实现WritableComparable 2、定义key的属性，getset方法 3、重写方法：序列化，反序列化，排序方法 4、
 */
public class MyKey implements WritableComparable<MyKey> {

	private Integer date;
	private String place;
	private Integer max;
	private Integer min;
	private Integer avg;
	private Integer max_diff;
	private Integer min_diff;
	private Integer avg_diff;

	public Integer getDate() {
		return date;
	}

	public void setDate(Integer date) {
		this.date = date;
	}

	public String getPlace() {
		return place;
	}

	public void setPlace(String place) {
		this.place = place;
	}

	public Integer getMax() {
		return max;
	}

	public void setMax(Integer max) {
		this.max = max;
	}

	public Integer getMin() {
		return min;
	}

	public void setMin(Integer min) {
		this.min = min;
	}

	public Integer getAvg() {
		return avg;
	}

	public void setAvg(Integer avg) {
		this.avg = avg;
	}

	public Integer getMax_diff() {
		return max_diff;
	}

	public void setMax_diff(Integer max_diff) {
		this.max_diff = max_diff;
	}

	public Integer getMin_diff() {
		return min_diff;
	}

	public void setMin_diff(Integer min_diff) {
		this.min_diff = min_diff;
	}

	public Integer getAvg_diff() {
		return avg_diff;
	}

	public void setAvg_diff(Integer avg_diff) {
		this.avg_diff = avg_diff;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(date);
		out.writeUTF(place);
		out.writeInt(max);
		out.writeInt(min);
		out.writeInt(avg);
		out.writeInt(max_diff);
		out.writeInt(min_diff);
		out.writeInt(avg_diff);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readInt();
		place = in.readUTF();
		max = in.readInt();
		min = in.readInt();
		avg = in.readInt();
		max_diff = in.readInt();
		min_diff = in.readInt();
		avg_diff = in.readInt();
	}

	@Override
	public int compareTo(MyKey o) {
		// 首先按照年份进行排序，然后按照最高气温进行排序
		// 年份大的排后面，年份小的排前面，年份相同的按照气温排
		// 最大的排后面，最小的排前面，相同的按照最小气温排
		// 最大的排后面，最小的排前面，相同的挨着
		if (date.intValue() != o.date.intValue())
			return date - o.date;
		// 年份相同的情况下
		if(max.intValue() !=o.max.intValue())
		return o.max - max;
		return o.min - min;
	}
	
	//重写hashCode方法,目的是HashPartition 默认分区函数计算出来的值只要年份一样得出的结果就一样,一个分区
	@Override
	public int hashCode() {
		return date;
	}
	@Override
	public String toString() {
		return date + "\t" + place + "\t" + max + "\t" + min + "\t" + avg + "\t" + max_diff + "\t" + min_diff + "\t"
				+ avg_diff;
	}
}
