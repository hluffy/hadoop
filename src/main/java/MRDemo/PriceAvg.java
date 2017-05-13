package MRDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 山西省计算每天均价
 * @author xhan
 *
 */
public class PriceAvg {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2){
			System.out.println("please input full path");
			System.exit(0);
		}
		Job job = new Job(new Configuration(),"PriceAvg");
		job.setJarByClass(PriceAvg.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(PriceMap.class);
		job.setReducerClass(PriceReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.waitForCompletion(true);
	}
	
	public static class PriceMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] lines = value.toString().split("\t");
			if(lines.length==6){
				if(lines[4].trim().equals("山西")){
					//将农产品名称和采购时间作为key输出，将价格作为value输出
					context.write(new Text(lines[0].trim()+"\t"+lines[2].trim()), new DoubleWritable(Double.parseDouble(lines[1].trim())));
				}
			}
		}
	}
	
	public static class PriceReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
				Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			double max = 0d;//最大值
			double min = Double.MAX_VALUE;//最小值
			double sum = 0;//价格总和
			int count = 0;//价格数量
			double avg;//价格平均值
			for (DoubleWritable value : values) {
				double price = value.get();
				sum = sum + price;
				if(max<price){
					max = price;//求出最大值
				}
				if(min>price){
					min = price;//求出最小值
				}
				count ++;//求出价格的数量
			}
			if(count>2){
				avg = (sum-max-min)/(count-2);
			}else{
				avg = sum/count;
			}
			context.write(key, new DoubleWritable(avg));
		}
	}

}
