package cp2016.pagerank.diff;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RowReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable key,
			Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double diff = 0.0;
		for (DoubleWritable val : values) {
			diff += val.get();
		}
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(context.getConfiguration().get("diffOutputFile"));
		OutputStreamWriter writer = new OutputStreamWriter(fs.create(path, true));
		BufferedWriter bw = new BufferedWriter(writer);
        bw.write(Double.toString(diff) + "\n");
        
        bw.close();
		writer.close();
	}
}
