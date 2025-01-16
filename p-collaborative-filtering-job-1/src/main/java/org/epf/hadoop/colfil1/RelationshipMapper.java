package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text id1 = new Text();
    private Text id2 = new Text();

    @Override
    protected void map(LongWritable cle, Relationship valeur, Context context) throws IOException, InterruptedException {
        id1.set(valeur.getId1());
        id2.set(valeur.getId2());

        context.write(id1, id2);
        context.write(id2, id1);
    }
}


