package com.gk.bdweek4.lab1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	private Text uniqueCarrier;
	private IntWritable depDelay;

	public KeyPair()
	{
		depDelay= new IntWritable();
		uniqueCarrier = new Text();
	}
	
	public KeyPair(String bId, Integer stars)
	{
		uniqueCarrier = new Text(bId);
		depDelay= new IntWritable(stars);
	}
	
	public void readFields(DataInput in) throws IOException {
		depDelay.readFields(in);
		uniqueCarrier.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException {
		depDelay.write(out);
		uniqueCarrier.write(out);
	}


	public int compareTo(KeyPair otherPair) {
		int c= uniqueCarrier.compareTo(otherPair.uniqueCarrier);
		if (c!=0)
			return c;
		else
			return depDelay.compareTo(otherPair.depDelay);
	}

	/**
	 * @return the uniqueCarrier
	 */
	public Text getUniqueCarrier() {
		return uniqueCarrier;
	}

	/**
	 * @param uniqueCarrier the uniqueCarrier to set
	 */
	public void setUniqueCarrier(Text uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	/**
	 * @return the depDelay
	 */
	public IntWritable getDepDelay() {
		return depDelay;
	}

	/**
	 * @param depDelay the depDelay to set
	 */
	public void setDepDelay(IntWritable depDelay) {
		this.depDelay = depDelay;
	}

}
