package com.gk.bdweek4.lab1;
import org.apache.hadoop.io.*;

/***
 * 
 * @author gauravkhandave
 *
 */
public class UniqueCarrierComparator extends WritableComparator{
	
	public UniqueCarrierComparator() {
		super(KeyPair.class, true);
		
	}
	@Override
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getUniqueCarrier().compareTo(key2.getUniqueCarrier());
	}
}
