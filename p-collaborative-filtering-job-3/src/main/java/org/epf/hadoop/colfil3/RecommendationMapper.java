package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, UserRecommendation> {
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parti = value.toString().split("\t");
        if (parti.length != 2) return;

        String[] users = parti[0].split(",");
        if (users.length != 2) return;

        int amiComuns = Integer.parseInt(parti[1]);

        outputKey.set(users[0]);
        context.write(outputKey, new UserRecommendation(
                users[0], users[1], amiComuns));

        outputKey.set(users[1]);
        context.write(outputKey, new UserRecommendation(
                users[1], users[0], amiComuns));
    }
}