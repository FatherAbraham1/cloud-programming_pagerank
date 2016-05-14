package cp2016.pagerank.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ScoreLinkPair implements Writable {

	private DoubleWritable score;
	private Text linksJSON;
	
	
	public ScoreLinkPair() {
		this.score = new DoubleWritable(0.0);
		this.linksJSON = new Text("");
	}
	
	public ScoreLinkPair(double score, String linksJSON) {
		this.score = new DoubleWritable(score);
		this.linksJSON = new Text(linksJSON); 
	}
	
	public double getScore() {
		return score.get();
	}
	
	public String getLinksJSON() {
		return linksJSON.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		score.write(out);
		linksJSON.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score.readFields(in);
		linksJSON.readFields(in);
	}
}
