package io.datamass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words=line.split("\t");
        for(int i = 0; i < words.length; i++)
        {
            Text outputKey = new Text(words[i].toUpperCase().trim());
            IntWritable outputValue = new IntWritable(i);
            con.write(outputKey, outputValue);
        }

    }

}
