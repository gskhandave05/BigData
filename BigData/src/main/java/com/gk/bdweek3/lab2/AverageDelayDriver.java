package com.gk.bdweek3.lab2;



import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class AverageDelayDriver extends Configured implements Tool {
	
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new AverageDelayDriver(),args);
    System.exit(exitCode);
  }

public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.err.println("Usage: WordCount <input path> <output path>");
	      System.exit(-1);
	    }

//Initializing the map reduce job
	Job job= new Job(getConf());
	job.setJarByClass(AverageDelayDriver.class);
	job.setJobName("AverageDelay");
	

	//Setting the input and output paths.The output file should not already exist. 
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//Setting the mapper, reducer, and combiner classes
	job.setMapperClass(AverageDelayMapper.class);
	job.setReducerClass(AverageDelayReducer.class);
	//job.setCombinerClass(WordOccuranceCountReducer.class);

	//Setting the format of the key-value pair to write in the output file.
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	//Submit the job and wait for its completion
	return(job.waitForCompletion(true) ? 0 : 1);
}
/*
 * JSONParser parser = new JSONParser();
		try {
			JSONObject json = (JSONObject) parser.parse(value.toString());
			json.get("business_id");
			output.collect(new Text(json.get("business_id").toString()), new Text(json.get("user_id").toString()+" "+json.get("stars")));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 */
}
