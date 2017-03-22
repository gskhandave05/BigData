package com.gk.bdweek3.lab3;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RatingDriver extends Configured implements Tool {
	
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new RatingDriver(),args);
    System.exit(exitCode);
  }

public int run(String[] args) throws Exception {
	if (args.length != 2) {
	      System.err.println("Usage: WordCount <input path> <output path>");
	      System.exit(-1);
	    }

//Initializing the map reduce job
	Job job= new Job(getConf());
	job.setJarByClass(RatingDriver.class);
	job.setJobName("ratingmapreduce");
	

	//Setting the input and output paths.The output file should not already exist. 
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//Setting the mapper, reducer, and combiner classes
	job.setMapperClass(RatingMapper.class);
	job.setReducerClass(RatingReducer.class);
	//job.setCombinerClass(WordOccuranceCountReducer.class);

	//Setting the format of the key-value pair to write in the output file.
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
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
