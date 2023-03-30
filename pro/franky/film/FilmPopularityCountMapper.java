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

public class FilmPopularityCountMapper extends Mapper<Object, Text, Text, IntWritable> {

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
