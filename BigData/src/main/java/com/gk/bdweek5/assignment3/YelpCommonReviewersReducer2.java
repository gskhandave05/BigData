package com.gk.bdweek5.assignment3;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/***
 * 
 * @author gauravkhandave
 *
 */
public class YelpCommonReviewersReducer2 extends MapReduceBase
implements Reducer<KeyPair, Text, Text, Text> {
  public void reduce(KeyPair key, Iterator<Text> values, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException   {
    
    HashSet<String> set = new HashSet<>();
    while(values.hasNext()){
    	set.add(values.next().toString());
    }
    ArrayList<String> distinctUserIds = new ArrayList<>();
    distinctUserIds.addAll(set);
   
   output.collect(new Text(key.getFirstBusinessId().toString()+"\t"+key.getSecondBusinessId()), new Text(distinctUserIds.size()+""));

    
  }
	
	
}
