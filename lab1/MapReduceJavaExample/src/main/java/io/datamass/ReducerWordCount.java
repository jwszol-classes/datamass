package io.datamass;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ReducerWordCount extends Reducer<Text, IntWritable, Text, WritablePair>
{

    public static int COLUMNS = 9;

    private long mapperCounter;

    public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
    {
        Integer[] counters = new Integer[]{0,0,0,0,0,0,0,0,0};
        for(IntWritable value : values)
        {
            int count = counters[value.get()];
            counters[value.get()] = ++count;
        }
        for (int i = 0; i < COLUMNS; i++) {
            int count = counters[i];
            if (count > 0) {

                float perc = count * 100f / mapperCounter;
                con.write(word, new WritablePair(i, perc));
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException{
        JobClient c = new JobClient(context.getConfiguration());
        org.apache.hadoop.mapred.JobID mapJob = new org.apache.hadoop.mapred.JobID(context.getJobID().getJtIdentifier(), context.getJobID().getId());
        RunningJob currentJob = c.getJob(mapJob);
        mapperCounter = currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    }

}

