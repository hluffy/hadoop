package MRDemo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Mobile implements Writable{
	public int internet;
	public int callTime;
	public int mailCount;
	public Mobile(){
		
	}
	public Mobile(int internet, int callTime, int mailCount) {
		super();
		this.internet = internet;
		this.callTime = callTime;
		this.mailCount = mailCount;
	}


	@Override
	public String toString() {
		return "\t"+internet+"\t"+callTime+"\t"+mailCount;
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		//注意输入的顺序与输出的顺序要一致
		this.internet = in.readInt();
		this.callTime = in.readInt();
		this.mailCount = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.internet);
		out.writeInt(this.callTime);
		out.writeInt(this.mailCount);
	}
	public int getInternet() {
		return internet;
	}
	public void setInternet(int internet) {
		this.internet = internet;
	}
	public int getCallTime() {
		return callTime;
	}
	public void setCallTime(int callTime) {
		this.callTime = callTime;
	}
	public int getMailCount() {
		return mailCount;
	}
	public void setMailCount(int mailCount) {
		this.mailCount = mailCount;
	}

}
