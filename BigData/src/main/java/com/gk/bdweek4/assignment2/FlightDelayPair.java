/**
 * 
 */
package com.gk.bdweek4.assignment2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author gauravkhandave
 *
 */
public class FlightDelayPair implements Writable{
	
	private DoubleWritable partialSum;
	private IntWritable partialCount;
	
	public FlightDelayPair()
	{
		partialSum = new DoubleWritable(0);
		partialCount=new IntWritable(0);
	}
	
	public FlightDelayPair(double sum,int count){
		this.partialSum = new DoubleWritable(sum);
		this.partialCount = new IntWritable(count);
	}
	
	/**
	 * @return the partialSum
	 */
	public double getPartialSum() {
		return this.partialSum.get();
	}

	/**
	 * @param partialSum the partialSum to set
	 */
	public void setPartialSum(DoubleWritable partialSum) {
		this.partialSum = partialSum;
	}

	/**
	 * @return the partialCount
	 */
	public int getPartialCount() {
		return this.partialCount.get();
	}

	/**
	 * @param partialCount the partialCount to set
	 */
	public void setPartialCount(IntWritable partialCount) {
		this.partialCount = partialCount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		partialSum.write(out);
		partialCount.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		partialSum.readFields(in);
		partialCount.readFields(in);
		
	}

}
