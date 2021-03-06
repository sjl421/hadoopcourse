package com.paul.mr;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
/**
 *此例子是由两个字符串数据构成的一个类型
 * 前一个字符串表示城市1
 * 后一个字符串表示城市2 
 * */
 
public class CityTextPair implements WritableComparable<TextPair> {
	String city1 ;    //城市1
	String city2 ;    //城市2

	public CityTextPair(){}

	public CityTextPair(String city1,String city2){
		set(city1,city2) ;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		city1 = in.readUTF() ;
		city2 = in.readUTF() ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(city1);
		out.writeUTF(city2);
	}
	@Override
	public int compareTo(CityTextPair tp) {
		//TODO :用比较，这里省略
		return 1;
	}
	public String toString(){
		return city1 +","+city2;
	}
	public void set(String first,String second){
		this.city1 = first ;
		this.city2 = second ;
	}
	
	public String getCity1() {
		return city1;
	}
	
	public String getCity2() {
		return city2;
	}
}
