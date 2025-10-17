package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedCartesianProduct extends Configured implements Tool {

    // Mapper for Dataset A
    public static class DatasetAMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text record = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming each line in dataset A has the format: user_id,valueA
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                userId.set(fields[0]);  // user_id
                record.set("A:" + value.toString()); // Prefix with "A" to identify dataset
                context.write(userId, record);
            }
        }
    }

    // Mapper for Dataset B
    public static class DatasetBMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text record = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming each line in dataset B has the format: user_id,valueB
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                userId.set(fields[0]);  // user_id
                record.set("B:" + value.toString()); // Prefix with "B" to identify dataset
                context.write(userId, record);
            }
        }
    }

    // JoinMapper to create Cartesian Product
    public static class JoinMapper extends Mapper<Text, Text, Text, Text> {
        private List<String> datasetARecords = new ArrayList<>();
        private List<String> datasetBRecords = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            datasetARecords.clear();
            datasetBRecords.clear();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("A:")) {
                datasetARecords.add(value.toString().substring(2)); // Remove "A:"
            } else if (value.toString().startsWith("B:")) {
                datasetBRecords.add(value.toString().substring(2)); // Remove "B:"
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit Cartesian product results
            for (String recordA : datasetARecords) {
                for (String recordB : datasetBRecords) {
                    context.write(new Text(recordA), new Text(recordB));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Distributed Cartesian Product");
        job.setJarByClass(DistributedCartesianProduct.class);

        // Adding multiple input paths
        MultipleInputs.addInputPath(job, new Path(args[0]), FileInputFormat.class, DatasetAMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), FileInputFormat.class, DatasetBMapper.class);

        job.setMapperClass(JoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);  // Map-only job

        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path for the Cartesian product

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DistributedCartesianProduct(), args);
        System.exit(exitCode);
    }
}

