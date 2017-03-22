
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * 
 */

/**
 * @author gauravkhandave
 *
 */
public class MaxCancellationDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		JobClient client = new JobClient();
		JobConf conf = new JobConf(MaxCancellationDriver.class);

		conf.setJobName("maxCancellation");
		conf.setNumReduceTasks(3);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("temp"));

		conf.setMapperClass(MaxCancellationMapper.class);
		conf.setReducerClass(MaxCancellationReducer.class);

		client.setConf(conf);

		try {
			JobClient.runJob(conf);
			// The job configuration for running mapper1 which takes /temp as an input and reducer1
			// writes final output to /output
			JobConf conf2 = new JobConf(MaxCancellationDriver.class);

			conf2.setJobName("maxCancellation");
			conf2.setNumReduceTasks(3);

			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(conf2, new Path("temp"));
			FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

			conf2.setMapperClass(MaxCancellationMapper1.class);
			conf2.setReducerClass(MaxCancellationReducer1.class);

			client.setConf(conf2);
			JobClient.runJob(conf2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
