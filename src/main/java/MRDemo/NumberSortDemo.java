package MRDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 自定义排序
 * @author xhan
 *
 */
public class NumberSortDemo {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		
		Job job = new Job(new Configuration(),"NumberSortDemo");
		job.setJarByClass(NumberSortDemo.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(NumberMap.class);
		job.setReducerClass(NumberReduce.class);
		job.setOutputKeyClass(NumberSort.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.waitForCompletion(true);
		
	}
	
	public static class NumberMap extends Mapper<LongWritable, Text, NumberSort, NullWritable>{
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, NumberSort, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] lines = value.toString().split("\t");
			if(lines.length==2){
				long first = Long.parseLong(lines[0]);
				long second = Long.parseLong(lines[1]);
				NumberSort numberSort = new NumberSort(first,second);
				context.write(numberSort, NullWritable.get());
			}
		}
	}
	
	public static class NumberReduce extends Reducer<NumberSort, NullWritable, NumberSort, NullWritable>{
		@Override
		protected void reduce(
				NumberSort key,
				Iterable<NullWritable> values,
				Reducer<NumberSort, NullWritable, NumberSort, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
	}

}
