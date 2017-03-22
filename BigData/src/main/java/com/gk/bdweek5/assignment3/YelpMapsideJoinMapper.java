package com.gk.bdweek5.assignment3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author gauravkhandave
 *
 */
public class YelpMapsideJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, String> businessRatingDict = new HashMap<>();

	public void configure(JobConf job) {
		Path pt = new Path("yelp_business1.json");
		FileSystem fs;
		try {
			fs = FileSystem.get(job);

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			String line;
			JSONObject json;
			JSONParser parser = new JSONParser();
			while ((line = br.readLine()) != null) {
				try {
					json = (JSONObject) parser.parse(line.toString());
					String businessId = (String) json.get("business_id");
					String businessName = (String) json.get("name");
					businessRatingDict.put(businessId, businessName);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		try {
			JSONObject json;
			JSONParser parser = new JSONParser();
			json = (JSONObject) parser.parse(value.toString());

			String business_id = (String) json.get("business_id");
			String userId = (String) json.get("user_id");

			if (businessRatingDict.containsKey(business_id)) {
				String businessName = businessRatingDict.get(business_id);
				output.collect(new Text(userId), new Text(business_id + "(" + businessName + ")"));
			}

		} catch (JSONException | ParseException e) {
			e.printStackTrace();
		}

	}
}
