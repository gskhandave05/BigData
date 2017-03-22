
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author gauravkhandave
 *	This Mapper extracts uniqueCarrier, source, destination and cancelled columns from input csv.
 *	The map function maps the uniqueCarriers as a key with source, destination as a value. 
 */
public class MaxCancellationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		StringTokenizer record = new StringTokenizer(line, ",");
		String mapKey = "";
		int colCounter = 1;
		while (record.hasMoreTokens()) {
			String token = record.nextToken();
			// Check for ignoring headers of csv file
			if (token.equals("Cancelled")) {
				return;
			}
			if (colCounter == 9 || colCounter == 17 || colCounter == 18 || colCounter == 22) {
				switch (colCounter) {
				case 9:
					mapKey = mapKey + token + " ";
					break;
				case 17:
					mapKey = mapKey + token + ",";
					break;
				case 18:
					mapKey = mapKey + token;
					break;
				case 22:
					// Checks if flight for given route is cancelled or not
					if (Integer.parseInt(token) == 1) {
						output.collect(new Text(mapKey), new IntWritable(1));
					}
					break;
				}

			}
			colCounter++;
		}

	}

}
