package cp2016.pagerank.result;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cp2016.pagerank.common.TitleRankPair;

public class RowMapper extends Mapper<LongWritable, Text, TitleRankPair, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] rows = value.toString().split("\n");
		for (String row : rows) {
			String[] vals = row.split("\t");
			if (vals.length == 3) {
				double score = Double.parseDouble(vals[1]);
				context.write(new TitleRankPair(vals[0], score), new DoubleWritable(score));
			}
		}
	}
}
