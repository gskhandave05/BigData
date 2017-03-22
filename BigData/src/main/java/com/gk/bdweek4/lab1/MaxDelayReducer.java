package com.gk.bdweek4.lab1;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author gauravkhandave
 *
 */
public class MaxDelayReducer extends Reducer<KeyPair, Text, Text, Text>{
	private static Integer max = 0;
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		int count = 0;
		for(Text value:values){
			count++;
			String[] line = value.toString().split(",");
			if(count == 1){
				max = Integer.parseInt(line[2]);
			}
			if(Integer.parseInt(line[2]) >= max){
				max = Integer.parseInt(line[2]);
				context.write(new Text(key.getUniqueCarrier().toString()), new Text(line[0]+" "+line[1]+" "+max.toString()));
			}
		}
	}
}
