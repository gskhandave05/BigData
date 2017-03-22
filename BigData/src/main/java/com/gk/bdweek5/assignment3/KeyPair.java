package com.gk.bdweek5.assignment3;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/**
 * 
 * @author gauravkhandave
 *
 */
public class KeyPair implements WritableComparable<KeyPair>{
 
	private Text firstBusinessId;
	private Text secondBusinessId;
	
	public KeyPair()
	{
		secondBusinessId= new Text();
		firstBusinessId = new Text();
	}
	
	public KeyPair(String bId, String bId2)
	{
		firstBusinessId = new Text(bId);
		secondBusinessId= new Text(bId2);
	}
	
	public void readFields(DataInput in) throws IOException {
		secondBusinessId.readFields(in);
		firstBusinessId.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException {
		secondBusinessId.write(out);
		firstBusinessId.write(out);
	}


	public int compareTo(KeyPair otherPair) {
		int c= firstBusinessId.compareTo(otherPair.firstBusinessId);
		if (c!=0)
			return c;
		else
			return secondBusinessId.compareTo(otherPair.secondBusinessId);
	}

	/**
	 * @return the firstBusinessId
	 */
	public Text getFirstBusinessId() {
		return firstBusinessId;
	}

	/**
	 * @param firstBusinessId the firstBusinessId to set
	 */
	public void setFirstBusinessId(Text firstBusinessId) {
		this.firstBusinessId = firstBusinessId;
	}

	/**
	 * @return the userId
	 */
	public Text getSecondBusinessId() {
		return secondBusinessId;
	}

	/**
	 * @param userId the userId to set
	 */
	public void setSecondBusinessId(Text userId) {
		this.secondBusinessId = userId;
	}
	
	
	

}
