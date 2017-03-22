package com.gk.bdweek3.lab2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageDelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/// Replacing all digits and punctuation with an empty string
		String line = value.toString();
		// Extracting the words
		String[] record = line.split(",");
		if (record[8].equals("UniqueCarrier") || record[15].equals("NA")) {
			return;
		}
		// Emitting each word as a key and one as its value
	     context.write(new Text(record[8]), new IntWritable(Integer.parseInt(record[15])));


	}

}
