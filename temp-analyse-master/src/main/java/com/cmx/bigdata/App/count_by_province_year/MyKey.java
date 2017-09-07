package com.cmx.bigdata.App.count_by_province_year;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * 自定义key
 * 重写序列化方法，反序列化方法，排序方法
 * 
 * @author zdjy
 *
 */
public class MyKey implements WritableComparable<MyKey>{
	private String place;
	private String date;
	
	public String getPlace() {
		return place;
	}
	public void setPlace(String place) {
		this.place = place;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	/*
	 * 序列化方法，按照定义的顺序写
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(place);
		out.writeUTF(date);
	}
	/*
	 *反序列化方法，按照定义的顺序读 
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		place=in.readUTF();
		date=in.readUTF();
	}
	/*
	 * 排序方法
	 * 排序会影响分组
	 * 
	 * 如果结果相等，那么进入同一个reduce方法，
	 * 
	 */
	@Override
	public int compareTo(MyKey o) {
		String result=place+"\t"+date;
		String result2=o.getPlace()+"\t"+o.getDate();
		return result.compareTo(result2);
	}
	/*
	 * 默认分区通过key的hashCode的出来的相等
	 * HashPartitioner调用了hashCode方法
	 * 而默认的hashCode方法是Object的hashCode方法
	 */
	@Override
	public int hashCode() {
		return (place+"\t"+date).hashCode();
	}
	
}
