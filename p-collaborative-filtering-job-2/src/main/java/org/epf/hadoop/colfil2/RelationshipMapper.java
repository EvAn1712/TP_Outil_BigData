package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<Text, Text, UserPair, Text> {
    private final Text DIRECT_RELATION = new Text("DIRECT");

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String user = key.toString();
        String[] friends = value.toString().split(",");

        // On ecrit chaque paire possible dans la liste
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                UserPair pair = new UserPair(friends[i], friends[j]);
                context.write(pair, new Text(user));
            }

            // On ecrit les relations directes pour l'indicateur DIRECT
            context.write(new UserPair(user, friends[i]), DIRECT_RELATION);
        }
    }
}
