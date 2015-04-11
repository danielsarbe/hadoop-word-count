package org.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by danielsarbe on 1/18/14.
 */

public class WordCountReducerTest {

    private WordCountReducer reducer;
    private Reducer.Context context;

    @Before
    public void init() throws IOException, InterruptedException {
        reducer = new WordCountReducer();
        context = mock(Reducer.Context.class);
    }



    @Test
    public void testSingleWord() throws IOException, InterruptedException {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(4), new IntWritable(7));

        reducer.reduce(new Text("foo"), values, context);

        verify(context).write(new Text("foo"), new IntWritable(12));
    }

}