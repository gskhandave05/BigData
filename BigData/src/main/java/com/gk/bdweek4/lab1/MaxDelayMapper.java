package com.gk.bdweek4.lab1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxDelayMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		String uniqueCarrier = columns[8];
		try {
			Integer deptDelay = Integer.parseInt(columns[15]);
			String flightNo = columns[9];
			String srcDest = columns[16]+"|"+columns[17];
			context.write(new KeyPair(uniqueCarrier, deptDelay),new Text(flightNo+","+srcDest+","+deptDelay.toString()));
		} catch (NumberFormatException e) {

		}

	}
}
