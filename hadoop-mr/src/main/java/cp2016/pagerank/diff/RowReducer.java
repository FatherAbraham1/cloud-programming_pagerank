package cp2016.pagerank.diff;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.ReduceCounter;

public class RowReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, IntWritable> {
	@Override
	public void reduce(IntWritable key,
			Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double diff = 0.0;
		for (DoubleWritable val : values) {
			diff += val.get();
		}
		
		
		context.getCounter(ReduceCounter.CONVERGENCE).increment(diff >= 0.001 ? 0 : 1);
		
	}
}
