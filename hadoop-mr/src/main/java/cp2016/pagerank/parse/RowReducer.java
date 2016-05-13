package cp2016.pagerank.parse;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RowReducer extends Reducer<IntWritable, TitleLinkPair, Text, Text> {
	
	@Override
	public void reduce(IntWritable key,
			Iterable<TitleLinkPair> values, Context context)
			throws IOException, InterruptedException {
		long count = 0;
		for (TitleLinkPair p : values) {
			count += 1;
			context.write(new Text(p.getTitle()), new Text(p.getLinksJSON()));
		}
		System.out.println(count);
	}
	
	
	private void gg() {
//		Path path = new Path("tmp/keys");
//        FileSystem fs = FileSystem.get(context.getConfiguration());
//        OutputStreamWriter outStream = new OutputStreamWriter(fs.append(path));
//        BufferedWriter br = new BufferedWriter(outStream);
//        br.write(title + "\n");
//        br.close();
//		outStream.close();
	}
}
