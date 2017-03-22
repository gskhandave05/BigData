package com.gk.bdweek4.assignment2p2;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author gauravkhandave
 *
 */
public class MaxRatingMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONParser parser = new JSONParser();
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

		}
	}
}
