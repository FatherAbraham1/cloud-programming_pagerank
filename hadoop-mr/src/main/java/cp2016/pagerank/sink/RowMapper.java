package cp2016.pagerank.sink;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RowMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] vals = value.toString().split("\n");
		for(String val : vals) {
			String[] stuffs = val.split("\t");
			if (stuffs.length == 3) {
				context.write(new IntWritable(0), new DoubleWritable(Double.parseDouble(stuffs[1])));
			}
		}
	}
}
