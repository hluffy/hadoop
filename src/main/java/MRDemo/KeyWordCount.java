package MRDemo;

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

public class KeyWordCount {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=3){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job1 = new Job(new Configuration(),"KeyWordCount");
		job1.setJarByClass(KeyWordCount.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapperClass(KeyWordCountMap.class);
		job1.setReducerClass(KeyWordCountReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(new Configuration(),"KeyWordCount");
		job2.setJarByClass(KeyWordCount.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(KeyWordSortMap.class);
		job2.setReducerClass(KeyWordSortReduce.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.waitForCompletion(true);
	}
	//第一组map
	public static class KeyWordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length==6){
				context.write(new Text(lines[2].trim()), new IntWritable(1));
			}
		}
	}
	//第一组reduce
	public static class KeyWordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	//第二组map
	//第一组产生的最终结果作为第二组map中的输入内容
	public static class KeyWordSortMap extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length==2){
				//从第一组MapReduce产生的最终结果中取出关键词和搜索次数
				String keyWord = lines[0].trim();
				int count = Integer.parseInt(lines[1].trim());
				//在map输出中，将搜索次数作为key输出，将关键词作为value输出
				context.write(new IntWritable(count), new Text(keyWord));
			}
		}
	}
	//第二组reduce
	//第二组的shuffle，需要对map产生的中间结果进行处理
	//会根据key值进行排序，并将相同key值的value放大一个集合中
	public static class KeyWordSortReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (Text info : values) {
				context.write(key, info);
			}
		}
	}

}
