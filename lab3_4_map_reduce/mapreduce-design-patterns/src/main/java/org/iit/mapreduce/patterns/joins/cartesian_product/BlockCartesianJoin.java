package org.iit.mapreduce.patterns.joins.cartesian_product;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class BlockCartesianJoin {
    public static class BlockJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private List<String> blockB = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read each block of DataSet B and keep it small enough to fit in memory
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path cacheFile : cacheFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFile.toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        blockB.add(line);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String recordB : blockB) {
                context.write(null, new Text(recordB + value)); // Cartesian product for each block pair
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cartesian Join");

        job.setJarByClass(CartesianJoin.class);
        job.setMapperClass(CartesianJoin.CartesianMapper.class);
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
