package com.gk.bdweek4.assignment2p2;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author gauravkhandave
 *
 */
public class UserRatingReducer extends Reducer<KeyPair, Text, Text, Text>{
	private static Integer max = 0;
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		int count = 0;
		for(Text value:values){
			count++;
			String[] line = value.toString().split(",");
			if(count == 1){
				max = Integer.parseInt(line[1]);
			}
			if(Integer.parseInt(line[1]) == max){
				context.write(new Text(key.getBusinessId().toString()+" "+line[0]), new Text(max.toString()));
			}
		}
	}
}
