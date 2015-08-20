import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

/**
 * Hadoop MapReduce example showing high and low for a day across all stock symbols
 * 
 */

public class HighLowDayDriver extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(HighLowDayDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		String input, output;

		long startTime=System.currentTimeMillis();
		logger.error("程序开始运行" + startTime);
		
		Job job = new Job(getConf());
		job.setJarByClass(HighLowDayDriver.class);
		job.setJobName("High Low per Day");

		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/xxy/mapred/X"));
//		FileInputFormat.setInputPaths(job. new Path("NASDAQ_daily_prices_A.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/xxy/mapred/result/X"));
//		FileInputFormat.setInputPaths(job, new Path("NASDAQ_daily_prices_A.csv"));
//		FileOutputFormat.setOutputPath(job, new Path("a.out"));

		job.setMapperClass(HighLowDayMapper.class);
		job.setReducerClass(HighLowDayReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		long finishTime = System.currentTimeMillis();
		logger.error("程序结束运行，耗时：" + (finishTime-startTime) + "ms");
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long startTime=System.currentTimeMillis();
		HighLowDayDriver driver = new HighLowDayDriver();
		int exitCode = ToolRunner.run(driver, args);

		System.exit(exitCode);
		
	}
}