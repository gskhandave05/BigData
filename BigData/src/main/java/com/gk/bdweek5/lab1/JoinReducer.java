package com.gk.bdweek5.lab1;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author gauravkhandave
 *
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text>{
	private static Integer max = 0;
	public void reduce(Text key, Iterable<Text> values, Context context)
			  throws IOException, InterruptedException {
		
		ArrayList<String> mValues = new ArrayList<>();
		ArrayList<String> bValues = new ArrayList<>();
		
		for (Text v: values){
			String[] splits = v.toString().split(",");
			if(splits[0].equals("M")){
				mValues.add(splits[1]);
			} else {
				bValues.add(splits[1]);
			}
		}
		
		String outValue = "";
		
		for (String userRating:mValues){
			for (String businessName:bValues){
				//outValue = key + "," + userRating + "," + businessName;
				context.write(key, new Text(businessName+"\t"+userRating));
			}
		}
	}
}
