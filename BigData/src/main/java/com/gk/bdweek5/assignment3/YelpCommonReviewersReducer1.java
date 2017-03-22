package com.gk.bdweek5.assignment3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author gauravkhandave
 *
 */
public class YelpCommonReviewersReducer1 extends MapReduceBase
implements Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException  {	
		List<String> list = new ArrayList<>();
		while(values.hasNext()){
			list.add(values.next().toString());
		}
		for(int i=0;i<list.size();i++){
			for(int j=i+1;j<list.size();j++){
				output.collect(new Text(list.get(i)+";"+list.get(j)), key);
			}
		}
	}
}
