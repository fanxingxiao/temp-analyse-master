package com.cmx.bigdata.App.topNs2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey>{
	private int max;

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public MyKey(int max) {
		this.max = max;
	}

	public MyKey() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(max);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		max=in.readInt();
	}

	@Override
	public int compareTo(MyKey o) {
		return o.max-max;
	}
	
}
