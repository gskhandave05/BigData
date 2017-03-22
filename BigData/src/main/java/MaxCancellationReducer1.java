
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author gauravkhandave
 *	This class sorts the cancelled count and collects output with highest cancellation count.
 */
public class MaxCancellationReducer1 extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		Map<String,Integer> map = new HashMap<>();

		while (values.hasNext()) {
			String srcDestNCount = values.next().toString();
			
			String[] arr = srcDestNCount.split(" ");
			Integer count = Integer.parseInt(arr[2]);
			
			map.put(arr[1],count);
			
		}
		Integer max = Collections.max(map.values());
		
		for(Map.Entry<String, Integer> entry:map.entrySet()){
			if(entry.getValue()==max){
				output.collect(new Text(key+" "+entry.getKey()), new IntWritable(max));
			}
		}
	}

}
