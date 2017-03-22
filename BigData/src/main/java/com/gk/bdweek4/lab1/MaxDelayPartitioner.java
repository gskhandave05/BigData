package com.gk.bdweek4.lab1;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/***
 * 
 * @author gauravkhandave
 *
 */
public class MaxDelayPartitioner extends Partitioner<KeyPair, IntWritable>{

	@Override
	public int getPartition(KeyPair key, IntWritable value, int numReducers) {
		return (key.getUniqueCarrier().hashCode()& Integer.MAX_VALUE)% numReducers;
	
	}
	

}
