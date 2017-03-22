/**
 * 
 */
package com.gk.bdweek4.assignment2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author gauravkhandave
 *
 */
public class AverageFlightDelayCombiner extends Reducer<Text, FlightDelayPair, Text, FlightDelayPair> {

	public void reduce(Text key, Iterable<FlightDelayPair> values, Context context)
			throws IOException, InterruptedException {

		double sum = 0;
		int count = 0;
		for (FlightDelayPair value : values) {
			sum += value.getPartialSum();
			count += value.getPartialCount();
		}
		context.write(key, new FlightDelayPair(sum, count));
	}

}
