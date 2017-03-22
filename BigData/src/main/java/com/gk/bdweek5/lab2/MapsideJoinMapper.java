package com.gk.bdweek5.lab2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class MapsideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	HashMap<String, String> businessRatingDict = new HashMap<>();
	
	public void setup(Context context) throws IOException{
		Path pt = new Path("maxRating");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		
			String line;// = br.readLine();
			while((line = br.readLine())!=null){
				String[] splits = line.toString().split("\t");
				if (splits.length >=2){
					String[] tokens = splits[0].split(" ");
					String businessId = tokens[0];
					businessRatingDict.put(businessId, tokens[1]+"\t"+splits[1]);
				}
			}
	}
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
			  try{
				  JSONObject json;
				  JSONParser parser = new JSONParser();
				  json = (JSONObject) parser.parse(value.toString());
				  
				  String business_id=(String)json.get("business_id");
				  String business_name=(String)json.get("name");
				  //String userRating = businessRatingDict.get(business_id);

				  if (businessRatingDict.containsKey(business_id)){
					  String userRating = businessRatingDict.get(business_id);
					  context.write(new Text(business_id), new Text(business_name+"\t"+userRating));
				  }
				  
			  }
			  catch(JSONException | ParseException e)
			  {
				  e.printStackTrace();
			  }
		  
	}
}
