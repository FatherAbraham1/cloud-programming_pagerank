package cp2016.pagerank.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TitleRanksPair implements WritableComparable<TitleRanksPair> {

	private Text title;
	private DoubleWritable previousRank;
	private DoubleWritable currentRank;
	
	public TitleRanksPair() {
		this.title = new Text();
		this.previousRank = new DoubleWritable(0.0);
		this.currentRank = new DoubleWritable(0.0);
	}
	
	public TitleRanksPair(String title, double prevRank, double curRank) {
		this.title = new Text(title);
		this.previousRank = new DoubleWritable(prevRank);
		this.currentRank = new DoubleWritable(curRank);
	}
	
	public String getTitle() {
		return title.toString();
	}
	
	public double getPreviousRank() {
		return previousRank.get();
	}
	
	public double getCurrentRank() {
		return currentRank.get();
	}
	
	@Override
	public String toString() {
		return String.join("\t", title.toString(), previousRank.toString(), currentRank.toString());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		previousRank.write(out);
		currentRank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		previousRank.readFields(in);
		currentRank.readFields(in);
	}

	@Override
	public int compareTo(TitleRanksPair o) {
		int result = getTitle().compareTo(o.getTitle());
		if (result == 0) {
			result = Double.compare(getCurrentRank(), o.getCurrentRank());
		}
		return result;
	}

}
