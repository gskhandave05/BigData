

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 */

/**
 * @author gauravkhandave
 *	This class adds the cancelled flights for given routes.
 *	Collects the calculated output into /temp folder which acts as input to Mapper1 class.
 */
public class MaxCancellationReducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {

		int sum = 0;
	    //Summing up the counts for each cancellation
	    while(values.hasNext()) {
	      sum += Integer.parseInt(values.next().toString());
	    }
		output.collect(key, new IntWritable(sum));
	}

}
