package cp2016.pagerank.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.alibaba.fastjson.JSON;

public class TitleLinkPair implements Writable {

	private Text title;
	private Text linksJSON;
	private List<String> links = null;
	
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
	
	public List<String> getLinks() {
		if (links == null) {
			links = JSON.parseArray(this.linksJSON.toString(), String.class);
		}
		return links;
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
