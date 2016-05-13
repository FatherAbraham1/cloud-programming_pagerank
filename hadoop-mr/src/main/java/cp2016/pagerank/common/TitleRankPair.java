package cp2016.pagerank.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TitleRankPair implements WritableComparable<TitleRankPair> {

	private Text title;
	private DoubleWritable rank;
	
	public TitleRankPair() {
		this.title = new Text();
		this.rank = new DoubleWritable(0.0);
	}
	
	public TitleRankPair(String title, double rank) {
		this.title = new Text(title);
		this.rank = new DoubleWritable(rank);
	}
	
	public String getTitle() {
		return title.toString();
	}
	
	public double getRank() {
		return rank.get();
	}
	
	@Override
	public String toString() {
		return String.join("\t", title.toString(), rank.toString());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		rank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		rank.readFields(in);
	}

	@Override
	public int compareTo(TitleRankPair o) {
		int result = getTitle().compareTo(o.getTitle());
		if (result == 0) {
			result = Double.compare(getRank(), o.getRank());
		}
		return result;
	}

}
