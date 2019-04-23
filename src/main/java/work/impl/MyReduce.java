package work.impl;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<InfoWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(InfoWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text t : values) {
            i++;
            if (i > 3) {
                break;
            }
            context.write(t, NullWritable.get());
        }
    }
}