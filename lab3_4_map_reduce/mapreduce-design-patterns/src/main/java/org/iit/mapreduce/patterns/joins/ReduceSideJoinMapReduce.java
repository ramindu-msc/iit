package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceSideJoinMapReduce extends Configured implements Tool {

    // Mapper for customer data
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                String customerId = fields[0];
                String customerInfo = "CUSTOMER|" + fields[1] + "," + fields[2];
                context.write(new Text(customerId), new Text(customerInfo));
            }
        }
    }

    // Mapper for order data
    public static class OrderMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                String customerId = fields[1];
                String orderInfo = "ORDER|" + fields[0] + "," + fields[2];
                context.write(new Text(customerId), new Text(orderInfo));
            }
        }
    }

    // Reducer for joining customer and order data
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> customerInfoList = new ArrayList<>();
            List<String> orderInfoList = new ArrayList<>();

            // Separate customer and order information
            for (Text value : values) {
                String[] parts = value.toString().split("\\|", 2);
                if (parts[0].equals("CUSTOMER")) {
                    customerInfoList.add(parts[1]);
                } else if (parts[0].equals("ORDER")) {
                    orderInfoList.add(parts[1]);
                }
            }

            // Perform the join
            for (String customerInfo : customerInfoList) {
                for (String orderInfo : orderInfoList) {
                    context.write(new Text(key), new Text(customerInfo + "\t" + orderInfo));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReduceSideJoinMapReduce <customer input path> <order input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Reduce Side Join Example");

        job.setJarByClass(ReduceSideJoinMapReduce.class);

        // Set up multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);

        // Set reducer class
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceSideJoinMapReduce(), args);
        System.exit(exitCode);
    }
}

