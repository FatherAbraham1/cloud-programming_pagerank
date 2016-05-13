package cp2016.pagerank.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TitleLinkPair implements Writable {

	private Text title;
	private Text linksJSON;
	
	public TitleLinkPair() {
		title = new Text();
		linksJSON = new Text("[]");
	}
	
	public TitleLinkPair(String title, String linksJSON) {
		this.title = new Text(title);
		this.linksJSON = new Text(linksJSON);
	}
	
	public String getTitle() {
		return this.title.toString();
	}
	
	public String getLinksJSON() {
		return this.linksJSON.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		linksJSON.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		linksJSON.readFields(in);
	}

}
