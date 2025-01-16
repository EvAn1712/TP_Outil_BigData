package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable cle = new LongWritable();
    private Relationship valeur = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNext = lineRecordReader.nextKeyValue();
        if (hasNext) {
            cle.set(lineRecordReader.getCurrentKey().get());

            String ligne = lineRecordReader.getCurrentValue().toString();
            String[] partie = ligne.split("<->");
            if (partie.length == 2) {
                String[] idTimestamp = partie[1].split(",");
                valeur.setId1(partie[0].trim());
                valeur.setId2(idTimestamp[0].trim());
            } else {
                throw new IOException("Format ligne invalide : " + ligne);
            }
        }
        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return cle;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return valeur;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
