package org.hadoop.wordcount;

/**
 * Created by danielsarbe on 1/18/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class WordCount extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = new Job(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        //job.setInputFormatClass(CFInputFormat.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setPartitionerClass(HashPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileSystem fs = FileSystem.get(getConf());
//        Path[] inputPaths = InputUtils.getRecursivePaths(fs, args[0]);
//        LOGGER.info(Arrays.asList(inputPaths).toString());
//        FileInputFormat.setInputPaths(job, inputPaths);
        FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[0]));

        //FileInputFormat.setInputPathFilter(job, EnglishTextFileFilter.class);

        FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));
        //FileOutputFormat.setCompressOutput(job,true);
        //FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);

        long startTime = System.currentTimeMillis();
        int waitForCompletion = job.waitForCompletion(true) ? 0 : 1;
        LOGGER.info(getClass().getSimpleName() + " finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return waitForCompletion;

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

}