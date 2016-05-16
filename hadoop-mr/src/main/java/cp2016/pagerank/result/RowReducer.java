package cp2016.pagerank.result;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.TitleRanksPair;

public class RowReducer extends Reducer<TitleRanksPair, DoubleWritable, Text, DoubleWritable> {

	@Override 
	public void reduce(TitleRanksPair key,
			Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.getTitle()), new DoubleWritable(key.getCurrentRank()));
	}
	
}
