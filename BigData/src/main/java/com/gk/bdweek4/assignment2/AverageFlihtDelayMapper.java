package com.gk.bdweek4.assignment2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author gauravkhandave
 *
 */
public class AverageFlihtDelayMapper extends Mapper<LongWritable, Text, Text, FlightDelayPair> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] columns = value.toString().split(",");
		String uniqueCarrier = columns[8];
		try {
			int deptDelay = Integer.parseInt(columns[15]);
			context.write(new Text(uniqueCarrier), new FlightDelayPair(deptDelay, 1));
		} catch (NumberFormatException e) {

		}

	}
}
