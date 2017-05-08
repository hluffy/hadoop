package HDFSDemo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileCopy {
	public static void main(String[] args) throws Exception {
		String localPath = args[0];//源路径
		String toPath = args[1];//目标路径
		//创建输入流
		InputStream inputStream = new BufferedInputStream(new FileInputStream(localPath));
		Configuration config = new Configuration();
		//创建FileSystem对象
		FileSystem fs = FileSystem.get(URI.create(toPath), config);
		//创建输出流
		OutputStream outputStream = fs.create(new Path(toPath));
		//将输入流的内容复制到输出流中
		IOUtils.copyBytes(inputStream, outputStream,4096, false);
		
		IOUtils.closeStream(inputStream);
		IOUtils.closeStream(outputStream);
	}

}
