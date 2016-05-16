package cp2016.pagerank.sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RowReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {
	@Override 
	public void reduce(IntWritable key,
			Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long numTitles = context.getConfiguration().getLong("numberOfTitles", 0);
		if (numTitles == 0) {
			throw new RuntimeException("Number of titles is 0!");
		}
		
		double score = 0.0;
		for(DoubleWritable d : values) {
			score += d.get();
		}
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(context.getConfiguration().get("sinkNodeOutputFile"));
		OutputStreamWriter writer = new OutputStreamWriter(fs.create(path, true));
		BufferedWriter bw = new BufferedWriter(writer);
        bw.write(Double.toString(score / numTitles) + "\n");
        
        bw.close();
		writer.close();
	}
}
