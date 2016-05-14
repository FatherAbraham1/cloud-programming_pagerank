package cp2016.pagerank.parse;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Base64.*;

import cp2016.pagerank.common.TitleLinkPair;

public class RowReducer extends Reducer<IntWritable, TitleLinkPair, Text, Text> {
	
	@Override
	public void reduce(IntWritable key,
			Iterable<TitleLinkPair> values, Context context)
			throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());	
		Encoder encoder = java.util.Base64.getUrlEncoder();
		
		for (TitleLinkPair p : values) {
			String title = p.getTitle();
			
			Path path = new Path("tmp/titles/" + encoder.encodeToString(title.getBytes()));
			fs.create(path, true);
			
			context.write(new Text(title), new Text(p.getLinksJSON()));
		}
		
	}
}
