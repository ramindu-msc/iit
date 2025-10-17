package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReplicatedJoin {
    public class ReplicatedJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> departmentMap = new HashMap<>();

        /**
         * This loads the file that can be stored in Memory
         */
        @Override
        protected void setup(Context context) throws IOException {
            URI[] uris = context.getCacheFiles();
            if (uris != null && uris.length > 0) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        context.getConfiguration().get("fs.defaultFS").equals("file:///")
                                ? new FileInputStream(uris[0].getPath())
                                : new FileInputStream(new Path(uris[0].getPath()).toString())
                ));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    departmentMap.put(parts[0], parts[1]);
                }
                reader.close();
            }
        }

        /**
         * This will load the records in the large file distributed across the HDFS
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String empId = fields[0];
            String empName = fields[1];
            String deptId = fields[2];

            String deptName = departmentMap.get(deptId);
            if (deptName != null) {
                context.write(new Text(empId), new Text(empName + "\t" + deptName));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReplicatedJoinDriver <small-dataset> <large-dataset> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Replicated Join");
        job.setJarByClass(ReplicatedJoin.class);
        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);  // No reducer is needed

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.addCacheFile(new Path(args[0]).toUri());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

