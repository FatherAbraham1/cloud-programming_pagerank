package cp2016.pagerank.parse;

import java.io.IOException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.TitleLinkPair;

public class RowReducer extends Reducer<IntWritable, TitleLinkPair, Text, Text> {
	
	
	@Override
	public void reduce(IntWritable key,
			Iterable<TitleLinkPair> values, Context context)
			throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());	
		
		for (TitleLinkPair p : values) {
			String title = p.getTitle();
			
			Path path = new Path("tmp/titles/" + DigestUtils.sha256Hex(title));
			fs.create(path, true);
			
			context.write(new Text(title), new Text(p.getLinksJSON()));
			System.gc();
		}
		
	}
}
