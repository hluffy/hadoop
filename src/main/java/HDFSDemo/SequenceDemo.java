package HDFSDemo;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


public class SequenceDemo {
	//创建字符串型的数组
	private static final String[] DATA = {
		"hello","Hadoop","Spark","World","WordCount"
	};
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration config = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(uri),config);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fileSystem,config,path,key.getClass(), value.getClass());
			for(int i =0;i<10;i++){
				key.set(10-i);
				value.set(DATA[i%DATA.length]);
				System.out.printf("%s\t%s\t%s\n", writer.getLength(),key,value);
				writer.append(key, value);
			}
		} finally{
			IOUtils.closeStream(writer);
		}
		
	}

}
