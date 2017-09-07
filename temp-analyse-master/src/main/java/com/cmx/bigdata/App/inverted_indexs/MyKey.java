package com.cmx.bigdata.App.inverted_indexs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;


/**
 * 自定义类 1、写一个类，实现WritableComparable 2、定义key的属性，getset方法 3、重写方法：序列化，反序列化，排序方法 4、
 */
public class MyKey implements WritableComparable<MyKey> {

	private String word;
	private String name;
	private Integer quantity;


	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeUTF(name);
		out.writeInt(quantity);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word = in.readUTF();
		name = in.readUTF();
		quantity = in.readInt();
	}

	@Override
	public int compareTo(MyKey o) {
		return o.quantity - quantity;
	}

	@Override
	public String toString() {
		return quantity + "\t" + name + "\t" +word;
	}
}
