package org.iit.mapreduce.patterns.sampling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;

public class SimpleRandomSampling extends Configured implements Tool {
	public static final String FILTER_PERCENTAGE_KEY = "filter_percentage";

	public static class SimpleRandomSamplingMapper extends
			Mapper<Object, Text, NullWritable, Text> {
		private float filterPercentage;
		private Random rands = new Random();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// 0.1 (10%), 0.25 (25%)
			// if the filter percentage is set to 95% (or 0.95 as a decimal) in the SimpleRandomSampling program,
			// it means that approximately 95% of the records in the input dataset will be included in the output sample.
			filterPercentage = context.getConfiguration().getFloat(
					FILTER_PERCENTAGE_KEY, 0.0f);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// A random float value is generated between 0.0 and 1.0 using
			if (rands.nextFloat() < filterPercentage) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new SimpleRandomSampling(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();
		if (otherArgs.length != 3) {
			printUsage();
		}
		Float filterPercentage = 0.0f;
		try {
			filterPercentage = Float.parseFloat(otherArgs[0]) / 100.0f;
		} catch (NumberFormatException nfe) {
			printUsage();
		}

		Job job =  Job.getInstance(conf, "Simple Random Sampling");
		job.setJarByClass(SimpleRandomSampling.class);
		job.setMapperClass(SimpleRandomSamplingMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1); // prevent lots of small files
		job.getConfiguration()
				.setFloat(FILTER_PERCENTAGE_KEY, filterPercentage);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	private void printUsage() {
		System.err
				.println("Usage: SimpleRandomSampling <percentage> <in> <out>");
		ToolRunner.printGenericCommandUsage(System.err);
		System.exit(2);
	}
}