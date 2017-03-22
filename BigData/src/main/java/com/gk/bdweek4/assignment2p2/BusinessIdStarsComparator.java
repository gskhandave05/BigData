package com.gk.bdweek4.assignment2p2;
import org.apache.hadoop.io.*;

/**
 * 
 * @author gauravkhandave
 *
 */
public class BusinessIdStarsComparator extends WritableComparator {
	
	public BusinessIdStarsComparator() {
		super(KeyPair.class, true);	
	}
	
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getBusinessId().compareTo(key2.getBusinessId());

		if (c==0)
			return -key1.getRating().compareTo(key2.getRating());
		else
			return c;
	}
}
