package com.gk.bdweek3.lab3;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
	  Map<String,Integer> map = new HashMap<>();
	  
	  for(Text value:values){
		  String[]tokens = value.toString().split(" ");
		  map.put(tokens[0], Integer.parseInt(tokens[1]));
	  }
	  
	  List<String> mapKeys = new ArrayList<>(map.keySet());
	    List<Integer> mapValues = new ArrayList<>(map.values());
	    Collections.sort(mapValues);
	    Collections.sort(mapKeys);
	  LinkedHashMap<String, Integer> sortedMap =
		        new LinkedHashMap<>();

		    Iterator<Integer> valueIt = mapValues.iterator();
		    while (valueIt.hasNext()) {
		        Integer val = valueIt.next();
		        Iterator<String> keyIt = mapKeys.iterator();

		        while (keyIt.hasNext()) {
		            String k = keyIt.next();
		            Integer comp1 = map.get(k);
		            Integer comp2 = val;

		            if (comp1.equals(comp2)) {
		                keyIt.remove();
		                sortedMap.put(k, val);
		                break;
		            }
		        }
		    }
	  
		    List<Entry<String,Integer>> entryList =
		    	    new ArrayList<Map.Entry<String, Integer>>(sortedMap.entrySet());
		    	Entry<String, Integer> lastEntry =
		    	    entryList.get(entryList.size()-1);
    context.write(new Text(key.toString()+" "+lastEntry.getKey()), new Text(lastEntry.getValue().toString()));
  }
}
