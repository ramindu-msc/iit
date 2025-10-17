package org.iit.mapreduce.patterns.joins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class CompositeJoin {

    public class CompositeJoinMapper extends Mapper<Text, Text, Text, Text> {
        /**
         * Since the data is partitioned and sorted with the help of
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // key: departmentId, value: <source>,<record>
            String[] parts = value.toString().split(",", 2);
            String source = parts[0]; // EMP or DEPT
            String record = parts[1]; 
            context.write(new Text(key.toString()), new Text(source + "," + record));
        }
    }

    /**
     * departments.txt (sorted and partitioned by departmentId)
         * 1,HR
         * 2,Finance
         * 3,IT
     * employees.txt (sorted and partitioned by departmentId)
         * 1001,John,1
         * 1002,Jane,2
         * 1003,Doe,3
         * 1004,Smith,1
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CompositeJoinDriver <departments path> <employees path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Set the CompositeInputFormat
        String joinExpression = CompositeInputFormat.compose(
                "inner",
                KeyValueTextInputFormat.class,
                args[0], // /path/to/departments - pationg region
                args[1]  // /path/to/employees - patrtion regions
        );
        conf.set("mapreduce.join.expr", joinExpression);

        Job job = Job.getInstance(conf, "Composite Join");
        job.setJarByClass(CompositeJoin.class);

        job.setMapperClass(CompositeJoinMapper.class);
        job.setNumReduceTasks(0); // No reduce phase needed

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job, args[0] + "," + args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
