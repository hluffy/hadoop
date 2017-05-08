package HDFSDemo;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.zookeeper.common.IOUtils;
/**
 * 通过URL读取文件内容
 * @author xhan
 *
 */
public class URLCat {
	//让java程序能识别hadoop的hdfs url
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws MalformedURLException, IOException {
		InputStream inputStream = null;
		try {
			//此处的路径要写全路径
			inputStream = new URL(args[0]).openStream();
			/**
			 * 第一个参数是输入流
			 * 第二个参数是输出流
			 * 第三个参数是缓冲区大小
			 * 第四个参数是操作完成后是否关闭流
			 */
			IOUtils.copyBytes(inputStream, System.out, 4096,false);
			
		} finally{
			//关闭输入流
			IOUtils.closeStream(inputStream);
		}
		
		
	}

}
