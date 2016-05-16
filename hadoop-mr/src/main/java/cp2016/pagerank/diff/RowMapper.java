package cp2016.pagerank.diff;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException { 
		String[] str = value.toString().split("\n");
		double diff = 0.0;
		for (String val : str) {
			String[] tokens = val.split("\t");
			if (tokens.length == 4) {
				double oldVal = Double.parseDouble(tokens[1]);
				double newVal = Double.parseDouble(tokens[2]);
				diff += Math.abs(oldVal - newVal);
			}
		}
		context.write(new IntWritable(0), new DoubleWritable(diff));
	}

}
