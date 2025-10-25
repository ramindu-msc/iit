package org.iit.mapreduce.patterns.joins.cartesian_product;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CartesianProduct {

    // ------------------------ MAPPER ------------------------
    public static class CartesianMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable oneKey = new IntWritable(1);
        private Text outValue = new Text();
        private String datasetTag;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Identify which dataset this mapper is processing
            String fileName = context.getInputSplit().toString();
            if (fileName.contains("datasetA")) {
                datasetTag = "A";
            } else {
                datasetTag = "B";
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            outValue.set(datasetTag + ":" + value.toString());
            context.write(oneKey, outValue);
        }
    }

    // ------------------------ REDUCER ------------------------
    public static class CartesianReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> listA = new ArrayList<>();
            ArrayList<String> listB = new ArrayList<>();

            for (Text val : values) {
                String record = val.toString();
                if (record.startsWith("A:")) {
                    listA.add(record.substring(2));
                } else if (record.startsWith("B:")) {
                    listB.add(record.substring(2));
                }
            }

            for (String a : listA) {
                for (String b : listB) {
                    context.write(new Text("(" + a + ", " + b + ")"), NullWritable.get());
                }
            }
        }
    }

    // ------------------------ DRIVER ------------------------
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CartesianProduct <inputA> <inputB> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cartesian Product");

        job.setJarByClass(CartesianProduct.class);
        job.setMapperClass(CartesianMapper.class);
        job.setReducerClass(CartesianReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

