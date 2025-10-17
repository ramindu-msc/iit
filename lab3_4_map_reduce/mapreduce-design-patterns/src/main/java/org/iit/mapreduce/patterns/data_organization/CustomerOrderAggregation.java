package org.iit.mapreduce.patterns.data_organization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomerOrderAggregation {

    // Partitioned Input by Country:
    //      The data directory is organized by country (e.g., /data/USA/, /data/Canada/).
    //      The job input path is set to /data/USA/, so it only processes "USA" data.
    //Mapping Step:
    //      The input data is split across multiple mappers based on HDFS blocks within /data/USA/.
    //      Each mapper processes records for customers and orders and emits records with customer_id as the key and details as the value.
    //Reduce Step:
    //      Each reducer receives all records for a specific customer_id within the "USA" partition.
    public static class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text customerId = new Text();
        private Text customerData = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Customer data format: customer_id,name,address,country
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
                customerId.set(fields[0]); // customer_id
                customerData.set("CUST|" + fields[1] + "|" + fields[2] + "|" + fields[3]); // "CUST|name|address|country"
                context.write(customerId, customerData);
            }
        }
    }

    public static class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text customerId = new Text();
        private Text orderData = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Order data format: customer_id,order_id,amount
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                customerId.set(fields[0]); // customer_id
                orderData.set("ORDER|" + fields[1] + "|" + fields[2]); // "ORDER|order_id|amount"
                context.write(customerId, orderData);
            }
        }
    }

    // This output is written to a country-specific directory, such as /output/USA/
    public static class CustomerOrderReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = "", address = "", country = "";
            List<String> orders = new ArrayList<>();

            for (Text value : values) {
                String[] parts = value.toString().split("\\|");
                if (parts[0].equals("CUST")) {
                    name = parts[1];
                    address = parts[2];
                    country = parts[3];
                } else if (parts[0].equals("ORDER")) {
                    String order = "{ \"order_id\": \"" + parts[1] + "\", \"amount\": " + parts[2] + " }";
                    orders.add(order);
                }
            }

            // Build JSON-like output for each customer
            StringBuilder jsonOutput = new StringBuilder();
            jsonOutput.append("{ ")
                    .append("\"customer_id\": \"").append(key.toString()).append("\", ")
                    .append("\"name\": \"").append(name).append("\", ")
                    .append("\"address\": \"").append(address).append("\", ")
                    .append("\"country\": \"").append(country).append("\", ")
                    .append("\"orders\": [");
            jsonOutput.append(String.join(", ", orders));
            jsonOutput.append("] }");

            result.set(jsonOutput.toString());

            // Write output to country-specific folder using MultipleOutputs
            context.write(new Text(key), new Text(result));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Order Aggregation");
        job.setJarByClass(CustomerOrderAggregation.class);

        // Setting up multiple inputs
        MultipleInputs.addInputPath(job, new Path(args[0] + "/customers"), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0] + "/orders"), TextInputFormat.class, OrderMapper.class);

        job.setReducerClass(CustomerOrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

