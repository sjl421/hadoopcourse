package com.paul.mr;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CityTextPair implements WritableComparable<TextPair> {
	String city1 ;    //城市1
	String city2 ;    //城市2

	public TextPair(){}

	public TextPair(String city1,String city2){
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
	public int compareTo(TextPair tp) {
		return 1;
	}
	public String toString(){
		String str = String.valueOf(city1)+"、"+String.valueOf(city2) ;
		return str ;
	}
	public void set(String first,String second){
		this.city1 = first ;
		this.city2 = second ;
	}
	//============
	public String getCity1() {
		return city1;
	}
	public String getCity2() {
		return city2;
	}
}
