package com.gk.bdweek3.lab1;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordOccuranceCountMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   ///Replacing all digits and punctuation with an empty string
	  String line = value.toString().replaceAll("\\p{Punct}|\\d", "").toLowerCase();
   //Extracting the words
	  String[] record = line.split(" ");
   //Emitting each word as a key and one as its value
	  for (int i=0;i<record.length-1;i++){
		  String currentToken = record[i];
		  String nextToken = record[i+1];
		  if(currentToken.equals(" ") || nextToken.equals(" ")){
			  continue;
		  }
     context.write(new Text(currentToken +" "+ nextToken), new IntWritable(1));
	  }
    }
  }

