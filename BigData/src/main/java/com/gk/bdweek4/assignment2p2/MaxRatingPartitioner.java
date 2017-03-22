package com.gk.bdweek4.assignment2p2;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/***
 * 
 * @author gauravkhandave
 *
 */
public class MaxRatingPartitioner extends Partitioner<KeyPair, IntWritable>{

	@Override
	public int getPartition(KeyPair key, IntWritable value, int numReducers) {
		return (key.getBusinessId().hashCode()& Integer.MAX_VALUE)% numReducers;
	
	}
	

}
