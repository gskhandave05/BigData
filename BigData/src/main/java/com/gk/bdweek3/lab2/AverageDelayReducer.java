package com.gk.bdweek3.lab2;



import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class AverageDelayReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    //Summing up the counts for each word
    int count = 0;
    for (IntWritable value : values) {
      sum += value.get();
      count++;
    }
    context.write(key, new IntWritable(sum/count));
  }
}
