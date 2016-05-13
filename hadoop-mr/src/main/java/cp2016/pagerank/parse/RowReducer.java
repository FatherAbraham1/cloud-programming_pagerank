package cp2016.pagerank.parse;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

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
		
		Path path = new Path("tmp/titles");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		OutputStreamWriter outStream = new OutputStreamWriter(fs.create(path, true));
		BufferedWriter br = new BufferedWriter(outStream);
		
		for (TitleLinkPair p : values) {
			String title = p.getTitle();
			br.write(title + "\n");
			context.write(new Text(title), new Text(p.getLinksJSON()));
		}
		
		br.close();
		outStream.close();
	}
}
