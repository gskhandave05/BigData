package com.gk.bdweek4.lab1;
import org.apache.hadoop.io.*;

/**
 * 
 * @author gauravkhandave
 *
 */
public class CarrierDelayComparator extends WritableComparator {
	
	public CarrierDelayComparator() {
		super(KeyPair.class, true);	
	}
	
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getUniqueCarrier().compareTo(key2.getUniqueCarrier());

		if (c==0)
			return -key1.getDepDelay().compareTo(key2.getDepDelay());
		else
			return c;
	}
}
