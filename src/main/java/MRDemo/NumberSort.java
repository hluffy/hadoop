package MRDemo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumberSort implements WritableComparable<NumberSort>{
	public long first;
	public long second;
	public NumberSort(){
		
	}

	public NumberSort(long first, long second) {
		super();
		this.first = first;
		this.second = second;
	}

	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.first = input.readLong();
		this.second = input.readLong();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.first);
		out.writeLong(this.second);
	}

	//重写compareTo方法，改变排序规则
	public int compareTo(NumberSort o) {
		// TODO Auto-generated method stub
		long num = this.first-o.first;
		if(num!=0){
			return (int)num;
		}else{
			return (int)(this.second-o.second);
		}
	}

	@Override
	public String toString() {
		return first+" "+second;
	}

}
