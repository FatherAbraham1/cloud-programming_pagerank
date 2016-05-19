package cp2016.pagerank.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RanksLinkPair implements Writable {

	private DoubleWritable prevRank;
	private DoubleWritable curRank;
	private Text linksJSON;
	
	
	public RanksLinkPair() {
		this.prevRank = new DoubleWritable(0.0);
		this.curRank = new DoubleWritable(0.0);
		this.linksJSON = new Text("");
	}
	
	public RanksLinkPair(double prevRank, double curRank, String linksJSON) {
		this.prevRank = new DoubleWritable(prevRank);
		this.curRank = new DoubleWritable(curRank);
		this.linksJSON = new Text(linksJSON); 
	}
	
	public double getPreviousRank() {
		return prevRank.get();
	}
	
	public double getCurrentRank() {
		return curRank.get();
	}
	
	public String getLinksJSON() {
		return linksJSON.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		prevRank.write(out);
		curRank.write(out);
		linksJSON.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		prevRank.readFields(in);
		curRank.readFields(in);
		linksJSON.readFields(in);
	}
}
