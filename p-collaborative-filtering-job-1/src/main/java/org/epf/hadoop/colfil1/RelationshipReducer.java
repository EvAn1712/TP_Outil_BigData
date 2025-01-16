package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> valeurs, Context context) throws IOException, InterruptedException {
        Set<String> relations = new TreeSet<>();
        for (Text valeur : valeurs) {
            relations.add(valeur.toString());
        }

        String joinedRelations = String.join(",", relations);
        context.write(key, new Text(joinedRelations));
    }
}
