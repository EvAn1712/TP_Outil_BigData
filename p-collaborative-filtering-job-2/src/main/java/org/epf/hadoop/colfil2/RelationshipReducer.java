package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RelationshipReducer extends Reducer<UserPair, Text, UserPair, Text> {
    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> mutualFriends = new HashSet<>();
        boolean hasDirectRelation = false;

        for (Text value : values) {
            if (value.toString().equals("DIRECT")) {
                hasDirectRelation = true;
            } else {
                mutualFriends.add(value.toString());
            }
        }

        // Si pas de relation directe, on ecrit la paire et le nombre de relations communes
        if (!hasDirectRelation && !mutualFriends.isEmpty()) {
            context.write(key, new Text(String.valueOf(mutualFriends.size())));
        }

    }
}
