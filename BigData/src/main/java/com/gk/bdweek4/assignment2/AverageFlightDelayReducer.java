package com.gk.bdweek4.assignment2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 * 
 * @author gauravkhandave
 *
 */
public class AverageFlightDelayReducer extends Reducer<Text, FlightDelayPair, Text, DoubleWritable> {
	
	public void reduce(Text key, Iterable<FlightDelayPair> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;

		for (FlightDelayPair value : values) {
			sum += value.getPartialSum();
			count += value.getPartialCount();
		}
		context.write(key, new DoubleWritable(sum / count));
	}
}
