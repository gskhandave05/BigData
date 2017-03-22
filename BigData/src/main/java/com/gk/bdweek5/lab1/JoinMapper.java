package com.gk.bdweek5.lab1;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author gauravkhandave
 *
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		/*JSONParser parser = new JSONParser();
		try {
			JSONObject json;
			json = (JSONObject) parser.parse(value.toString());
			if (json.get("type").equals("review")) {
				if (!json.get("business_id").equals(null) && !json.get("user_id").equals(null)
						&& !json.get("stars").equals(null)) {
					context.write(new KeyPair(json.get("business_id").toString(),Integer.parseInt(json.get("stars").toString())),new Text(json.get("user_id").toString()+","+json.get("stars").toString()));
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();

		}*/
		
		String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		
		if (fileName.equals("maxRating")){
			String[] splits = value.toString().split("\t");
			if (splits.length >=2){
				String[] tokens = splits[0].split(" ");
				String businessId = tokens[0];
				context.write(new Text(businessId), new Text("M"+","+tokens[1]+"\t"+splits[1]));
			}
			
		} else //line is coming from yelp_business.json file
		  {
			  try{
				  JSONObject json;
				  JSONParser parser = new JSONParser();
				  json = (JSONObject) parser.parse(value.toString());
				  
				  String business_id=(String)json.get("business_id");
				  String business_name=(String)json.get("name");

				  
				  context.write(new Text(business_id), new Text("B"+","+business_name));
			  }
			  catch(JSONException | ParseException e)
			  {
				  e.printStackTrace();
			  }
		  }
	}
}
