package org.iit.mapreduce.patterns.joins.bloomfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.iit.mapreduce.patterns.joins.ReplicatedJoin;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class CustomerOrderJoinWithBloomFilter {

    public static class OrderMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class CustomerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private BloomFilter bloomFilter = new BloomFilter();
        /**
         * Since we are using bloomfilter with orders with customer id,
         * only relevant customer ids will be sent to reducer
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load the Bloom filter from Distributed Cache
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (DataInputStream in = new DataInputStream(new FileInputStream(cacheFiles[0].toString()))) {
                    bloomFilter.readFields(in);
                }
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class CustomerOrderReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Order Join With Bloom Filter");

        job.setJarByClass(CustomerOrderJoinWithBloomFilter.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(CustomerOrderReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0); // No reducer needed for filtering

        // Add the Bloom filter file to the Distributed Cache
        DistributedCache.addCacheFile(new Path("customer_bloom_filter.bf").toUri(), job.getConfiguration());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

