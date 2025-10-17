package org.iit.mapreduce.patterns.data_organization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StructuredToHierarchical {

    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input: C001,John Doe,123 Elm St,USA
            // read hdfs : full data sample
            //algo > get all key
            //check with bloom film
            String[] fields = value.toString().split(",");
            if (fields.length == 4) {
                String customerId = fields[0];
                context.write(new Text(customerId), new Text("CUST:" + value.toString()));
            }
        }
    }

    public static class OrderMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Input: C001,O123,250
            String[] fields = value.toString().split(",");
            if (fields.length == 3) {
                String customerId = fields[0];
                context.write(new Text(customerId), new Text("ORDER:" + value.toString()));
            }
        }
    }

    public static class HierarchicalReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONObject customerJson = new JSONObject();
            List<JSONObject> ordersList = new ArrayList<>();

            for (Text value : values) {
                String record = value.toString();
                if (record.startsWith("CUST:")) {
                    // Process customer record
                    String[] fields = record.substring(5).split(",");
                    customerJson.put("customer_id", fields[0]);
                    customerJson.put("name", fields[1]);
                    customerJson.put("address", fields[2]);
                    customerJson.put("country", fields[3]);
                } else if (record.startsWith("ORDER:")) {
                    // Process order record
                    String[] fields = record.substring(6).split(",");
                    JSONObject orderJson = new JSONObject();
                    orderJson.put("order_id", fields[1]);
                    orderJson.put("amount", Integer.parseInt(fields[2]));
                    ordersList.add(orderJson);
                }
            }

            customerJson.put("orders", new JSONArray(ordersList));
            context.write(NullWritable.get(), new Text(customerJson.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Structured to Hierarchical Transformation");

        job.setJarByClass(StructuredToHierarchical.class);

        // Set separate mappers for customer and order inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);

        job.setReducerClass(HierarchicalReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
