package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            for (int index = 0; index < args.length; ++index)
            {
                System.out.println("args[" + index + "]: " + args[index]);
            }
            System.err.println("Invalid command");
            System.err.println("Usage: Job2 <input path> <output path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job = Job.getInstance(conf, "ColFilJob2");
        job.setJarByClass(ColFilJob2.class);

        job.setNumReduceTasks(2);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);

        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
