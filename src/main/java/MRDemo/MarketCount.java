package MRDemo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

public class MarketCount {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"MarketCount");
		job.setJarByClass(MarketCount.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MarketMap.class);
		job.setReducerClass(MarketReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
	}
	
	public static class MarketMap extends Mapper<LongWritable, Text, Text, Text>{
		Text province = new Text();//省份
		Text market = new Text();//农产品市场
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] lines = value.toString().split("\t");
			if(lines.length==6){
				province.set(lines[4].trim());
				market.set(lines[3].trim());
				context.write(province, market);
			}
		}
	}
	
	/**
	 * map产生的中间结果经过shuffle阶段处理之后变成以下形式
	 * <北京,{}>
	 * @author xhan
	 *
	 */
	public static class MarketReduce extends Reducer<Text, Text, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Set<String> set = new HashSet<String>();
			for(Text value:values){
				//通过HashSet的去重功能对数据进行去重
				set.add(value.toString());
			}
			if(set.size()>0){
				context.write(key, new IntWritable(set.size()));
			}
		}
	}

}
