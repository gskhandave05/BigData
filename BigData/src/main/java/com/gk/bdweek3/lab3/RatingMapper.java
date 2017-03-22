package com.gk.bdweek3.lab3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		JSONParser parser = new JSONParser();
		try {
			JSONObject json;
			json = (JSONObject) parser.parse(value.toString());
			if (json.get("type").equals("review")) {
				if (!json.get("business_id").equals(null) && !json.get("user_id").equals(null)
						&& !json.get("stars").equals(null)) {
					context.write(new Text(json.get("business_id").toString()),
							new Text(json.get("user_id").toString() + " " + json.get("stars").toString()));
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
