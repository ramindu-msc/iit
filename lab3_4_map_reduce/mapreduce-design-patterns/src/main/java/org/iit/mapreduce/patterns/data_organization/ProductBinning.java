package org.iit.mapreduce.patterns.data_organization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ProductBinning {

    public static class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {
        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String productId = fields[0];
            String productName = fields[1];
            String category = fields[2];  // Assume category is given, e.g., Electronics, Clothing, Groceries.

            // Write each record to the appropriate bin file based on category
            if (category.equals("household")) {
                multipleOutputs.write("Electronics", productId, value, "Electronics/part");
                multipleOutputs.write("Clothing", productId, value, "Clothing/part");
            } else if (category.equals("sdfssdg")) {
                multipleOutputs.write("Clothing", productId, value, "Clothing/part");
            } else if (category.equals("fgdgd")) {
                multipleOutputs.write("Groceries", productId, value, "Groceries/part");
                multipleOutputs.write("Groceries", productId, value, "dfvdfvdf/part");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Binning");
        job.setJarByClass(ProductBinning.class);
        job.setMapperClass(ProductMapper.class);
        job.setNumReduceTasks(0); // No reducer needed for binning

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Configure multiple output paths for each bin
        MultipleOutputs.addNamedOutput(job, "Electronics", FileOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Clothing", FileOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Groceries", FileOutputFormat.class, Text.class, Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

