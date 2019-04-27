package work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import work.impl.*;

/**
 * data test：  stock market set from chinese stock, US market set from kaggle
 * https://www.kaggle.com/borismarjanovic/price-volume-data-for-all-us-stocks-etfs/version/3#
 * column：  Date  open	high low close Volume OpenInt
 * <p>
 * target： find top 3 tradingVolume data each month for each year
 * 1.One reduce task each year;
 * 2.create custom database，storage as [year-month-date-tradingVolume];
 * 3.sort by [year-month-tradingVolume]descending
 * 4.set groups as each month in same year
 * 5.reduce and output the record。
 * <p>
 * output：Trading Volume top 3 for each month   output：part-r-00000
 */
public class RunJob {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);

            job.setOutputKeyClass(InfoWritable.class);
            job.setOutputValueClass(Text.class);

            //设置分区数
            job.setNumReduceTasks(1);

            //设置排序
            job.setSortComparatorClass(SortComparator.class);

            //set groups
            job.setGroupingComparatorClass(GroupComparator.class);

            FileInputFormat.addInputPath(job, new Path("src/main/resources/上证指数 日线.txt"));

            
            Path path = new Path("src/main/resources/test");
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            FileOutputFormat.setOutputPath(job, path);

            boolean b = job.waitForCompletion(true);
            if (b) {
                System.out.println("job success！");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

