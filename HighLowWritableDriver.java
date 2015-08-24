import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce example showing a custom Writable
 *
 */
public class HighLowWritableDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output;

		Job job = new Job(getConf());
		job.setJarByClass(HighLowWritableDriver.class);
		job.setJobName("High Low per Stock with day");
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/xxy/mapred/a"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/xxy/mapred/resultW"));

		job.setMapperClass(HighLowWritableMapper.class);
		job.setReducerClass(HighLowWritableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StockWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		HighLowWritableDriver driver = new HighLowWritableDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
