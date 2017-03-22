package com.gk.bdweek5.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author gauravkhandave
 *
 */
public class YelpCommonReviewersMapper extends MapReduceBase implements Mapper<LongWritable, Text, KeyPair, Text> {
	public void map(LongWritable key, Text value, OutputCollector<KeyPair, Text> output, Reporter reporter) throws IOException {
		
		String[] tokens = value.toString().split("\t");
		
		if (tokens.length >=2){
		String[] bIds = tokens[0].split(";");
		if(bIds[0].equals(bIds[1])){
			return;
		}
		output.collect(new KeyPair(bIds[0], bIds[1]), new Text(tokens[1]));
		}
	}
	
}
