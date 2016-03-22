package org.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hadoop.wordcount.WordCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by danielsarbe on 1/18/14.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    private final static IntWritable one = new IntWritable(1);
    public static Text word = new Text();
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }


}
