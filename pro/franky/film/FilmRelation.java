package pro.franky.film;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilmRelation {
    /**
     *
     * 经过在 map 函数处理，输出中间结果<word，1>的形式，在 reduce 函数中完成对每个单词的词频统计<word, frequency>
     * 第一次 map-reduce: 将电影计数
     */
    protected static Map filmPopularity = new HashMap();
    public static class FilmPopularityCount {
        public static void work() throws Exception {
            // 启动job任务
            Job job = Job.getInstance();
            job.setJobName("FilmPopularityCount");
            // 设置mapper类、Reducer类
            job.setJarByClass(FilmPopularityCount.class); // 设置程序类
            job.setMapperClass(FilmPopularityCountMapper.class); // 设置Mapper类
            job.setReducerClass(FilmPopularityCountReducer.class); // 设置Reducer类

            // 设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，所以key设置为"Text"类型，Value设置为"IntWritable"类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // hdfs文件系统
            Path in = new Path("/input/1.csv"); // 需要统计的文本所在位置
            Path out = new Path("/output1"); // 输出文件夹不能存在

            // 本地文件系统。若文件在本地文件系统，则替换为以下代码
            // Path in = new
            // Path("file:///usr/local/java/data/mapreduce_demo/input/data_click"); //
            // 用本地文件输入
            // Path out = new Path("file:///usr/local/java/data/mapreduce_demo/output"); //
            // 结果输出到本地，文件夹不能已经存在

            // 设置job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);

            // 无论程序是否执行成功，均强制退出
            // 如果程序成功运行，返回true，则程序返回0；如果程序执行失败，返回false，则程序返回1
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }


    public static class UserFilmSummary {
        public static void work() throws Exception {
            // 启动job任务
            Job job = Job.getInstance();
            job.setJobName("UserFilmSummary");
            // 设置mapper类、Reducer类
            job.setJarByClass(UserFilmSummary.class); // 设置程序类
            job.setMapperClass(UserFilmSummaryMapper.class); // 设置Mapper类
            job.setReducerClass(UserFilmSummaryReducer.class); // 设置Reducer类

            // 设置Job输出结果<key,value>的中key和value数据类型，因为结果是<单词,个数>，所以key设置为"Text"类型，Value设置为"IntWritable"类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // hdfs文件系统
            Path in = new Path("/input/1.csv"); // 需要统计的文本所在位置
            Path out = new Path("/output2"); // 输出文件夹不能存在

            // 本地文件系统。若文件在本地文件系统，则替换为以下代码
            // Path in = new
            // Path("file:///usr/local/java/data/mapreduce_demo/input/data_click"); //
            // 用本地文件输入
            // Path out = new Path("file:///usr/local/java/data/mapreduce_demo/output"); //
            // 结果输出到本地，文件夹不能已经存在

            // 设置job执行作业时输入和输出文件的路径
            FileInputFormat.addInputPath(job, in);
            FileOutputFormat.setOutputPath(job, out);

            // 无论程序是否执行成功，均强制退出
            // 如果程序成功运行，返回true，则程序返回0；如果程序执行失败，返回false，则程序返回1
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    public static class UserFilmSummaryMapper extends Mapper<Object, Text, Text, IntWritable> {

        public static final IntWritable one = new IntWritable(1);

        // 重写map方法，将k1,v1转为k2，v2
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // csv
            String[] splits = value.toString().split(",");
            context.write(new Text(splits[1]), one);
            for (String word : splits) {
                context.write(new Text(word), one);
            }
        }
    }

    public static class UserFilmSummaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 遍历集合，将集合中的数字相加
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            filmPopularity.put(key, result);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        FilmPopularityCount.work();
    }
}
