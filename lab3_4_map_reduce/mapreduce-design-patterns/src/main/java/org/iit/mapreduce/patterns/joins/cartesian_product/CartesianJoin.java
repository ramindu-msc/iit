package org.iit.mapreduce.patterns.joins.cartesian_product;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * In case one data file can be loaded into the memory
 */
public class CartesianJoin {

    public static class CartesianMapper extends Mapper<LongWritable, Text, Text, Text> {
        private List<String> dataSetB = new ArrayList<>();

        @Override
        protected void setup(Context context) throws java.io.IOException, InterruptedException {
            // Load Data Set B into memory from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        context.getConfiguration().get("fs.defaultFS").equals("file://") ?
                                new java.io.FileInputStream(new java.io.File(cacheFiles[0].toString().replace("file://", ""))) :
                                new java.io.FileInputStream(new java.io.File(cacheFiles[0].toString()))))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        dataSetB.add(line);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            // For each record in Data Set A, join with all records in Data Set B
            for (String recordB : dataSetB) {
                context.write(value, new Text(recordB));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cartesian Join");

        job.setJarByClass(CartesianJoin.class);
        job.setMapperClass(CartesianMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Load Data Set B in the Distributed Cache
        job.addCacheFile(new URI(args[1])); // Data Set B

        // Data Set A input path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}