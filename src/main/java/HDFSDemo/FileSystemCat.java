package HDFSDemo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * 通过FileSystem的方式读取文件内容
 * @author xhan
 *
 */
public class FileSystemCat {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration config = new Configuration();
		//通过FIleSystem的get()方法创建FIleSystem对象
		FileSystem fs = FileSystem.get(URI.create(uri), config);
		InputStream inputStream = null;
		try {
			inputStream = fs.open(new Path(uri));
			//读取输入流中的内容并输出
			IOUtils.copyBytes(inputStream, System.out, 4096,false);
		} finally{
			IOUtils.closeStream(inputStream);
		}
	}

}
