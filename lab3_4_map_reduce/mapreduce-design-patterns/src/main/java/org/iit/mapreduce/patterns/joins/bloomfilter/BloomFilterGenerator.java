package org.iit.mapreduce.patterns.joins.bloomfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static java.lang.Math.*;
import static java.lang.Math.log;

public class BloomFilterGenerator {

    public static class OrderBloomFilterMapper extends Mapper<Object, Text, Text, Text> {
        int n = 10; // number of items in the filter: do a count before
        float p = 0.7f; // false positive rate

        int m = calculateM(n, p); // # of bits in the filter
        int k = calculateK(n, m); // # of hash functions
        private BloomFilter bloomFilter = new BloomFilter(m, k, Hash.MURMUR_HASH);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String customerId = fields[0]; // Assuming customer ID is in the first field
            bloomFilter.add(new Key(customerId.getBytes()));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Write the bloom filter to an HDFS path or local file
            try (DataOutputStream out = new DataOutputStream(new FileOutputStream("customer_bloom_filter.bf"))) {
                bloomFilter.write(out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bloom Filter Generator");
        job.setJarByClass(BloomFilterGenerator.class);
        job.setMapperClass(OrderBloomFilterMapper.class);
        job.setNumReduceTasks(0); // No reducer needed
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static int calculateM(int n, float p) {
        return (int) ceil((n * log(p)) / log(1.0f / (pow(2.0f, log(2.0f)))));
    }

    private static int calculateK(int n, int m) {
        return (int) round(log(2.0f) * m / n);
    }
}

