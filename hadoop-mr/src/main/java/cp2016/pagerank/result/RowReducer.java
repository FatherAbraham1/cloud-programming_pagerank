package cp2016.pagerank.result;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.TitleRankPair;

public class RowReducer extends Reducer<TitleRankPair, DoubleWritable, Text, DoubleWritable> {

	@Override 
	public void reduce(TitleRankPair key,
			Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.getTitle()), new DoubleWritable(key.getRank()));
	}
	
}
