package CarsDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计车辆不同用途的数量分布
 * @author xhan
 *
 */
public class UsrCar {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"UsrCar");
		
		job.setJarByClass(UsrCar.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(CarsMap.class);
		job.setReducerClass(CarsReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
	}
	
	public static class CarsMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		IntWritable one = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length>10&&lines[10]!=null){
				//获取汽车的使用性质
				String useType = lines[10].trim();
				//map输出中间结果
				context.write(new Text(useType), one);
			}
		}
	}
	
	public static class CarsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for(IntWritable value:values){
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}
