package com.gk.bdweek5.assignment3;
import org.apache.hadoop.io.*;

/***
 * 
 * @author gauravkhandave
 *
 */
public class BusinessIdComparator extends WritableComparator{
	
	public BusinessIdComparator() {
		super(KeyPair.class, true);
		
	}
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getFirstBusinessId().compareTo(key2.getFirstBusinessId());
	}
}
