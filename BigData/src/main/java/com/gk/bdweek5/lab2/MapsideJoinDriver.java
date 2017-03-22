package com.gk.bdweek5.lab2;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author gauravkhandave
 *
 */
public class MapsideJoinDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
	
		int exitCode = ToolRunner.run(new Configuration(),new MapsideJoinDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.err.println("Usage: join <input path> <output path>");
		      System.exit(-1);
		    }
		    //Initializing the map reduce job
		    Job job= new Job(getConf());
		    job.setJarByClass(MapsideJoinDriver.class);
		    job.setJobName("Join");
		    job.setNumReduceTasks(0);

		    //Setting the input and output paths.The output file should not already exist. 
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    //Setting the mapper, reducer, and combiner classes
		    job.setMapperClass(MapsideJoinMapper.class);
		    //job.setReducerClass(JoinReducer.class);
		    
		    //Setting the output key value type of the mapper 
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    
		  //Setting the type of the key-value pair to write in the output file.
		    /*job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);*/
		   

		    /*//Setting the grouping and sorting comparator classes
		      job.setGroupingComparatorClass(BusinessIdComparator.class);
		      job.setSortComparatorClass(BusinessIdStarsComparator.class);*/
		    
		    //Setting the custom partitioner
//		  job.setPartitionerClass(MaxRatingPartitioner.class);

		    //Submit the job and wait for its completion
		    return(job.waitForCompletion(true) ? 0 : 1);
		  }

	}

