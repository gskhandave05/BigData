package com.gk.bdweek4.assignment2p2;
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
 
	private Text businessId;
	private IntWritable rating;
	
	public KeyPair()
	{
		rating= new IntWritable();
		businessId = new Text();
	}
	
	public KeyPair(String bId, Integer stars)
	{
		businessId = new Text(bId);
		rating= new IntWritable(stars);
	}
	
	public void readFields(DataInput in) throws IOException {
		rating.readFields(in);
		businessId.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException {
		rating.write(out);
		businessId.write(out);
	}


	public int compareTo(KeyPair otherPair) {
		int c= businessId.compareTo(otherPair.businessId);
		if (c!=0)
			return c;
		else
			return rating.compareTo(otherPair.rating);
	}

	/**
	 * @return the rating
	 */
	public IntWritable getRating() {
		return rating;
	}

	/**
	 * @param rating the rating to set
	 */
	public void setRating(IntWritable rating) {
		this.rating = rating;
	}

	public Text getBusinessId() {
		return businessId;
	}

	public void setBusinessId(Text businessId) {
		this.businessId = businessId;
	}
	
	

}
