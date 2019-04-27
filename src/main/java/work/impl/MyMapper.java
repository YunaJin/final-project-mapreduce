package work.impl;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map：read file  analysis the data to get its year,month,day, and volume
 */
public class MyMapper extends Mapper<LongWritable, Text, InfoWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] str = value.toString().split("\t");
            String[] date = str[0].split("/");
            int year = Integer.parseInt(date[2]);
            int month = Integer.parseInt(date[0]);
            int day = Integer.parseInt(date[1]);
            Integer volume = Integer.parseInt(str[5]);
            InfoWritable info = new InfoWritable();
            info.setYear(year);
            info.setMonth(month);
            info.setDay(day);
            info.setVolume(volume);
            context.write(info, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
