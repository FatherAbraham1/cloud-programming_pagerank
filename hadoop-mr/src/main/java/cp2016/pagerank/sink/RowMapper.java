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
		double sink = 0.0;
		for(String val : vals) {
			String[] stuffs = val.split("\t");
			if (stuffs.length == 4) {
				if (stuffs[3].equals("[]")) {
					sink += Double.parseDouble(stuffs[2]);
				}
			}
		}
		context.write(new IntWritable(0), new DoubleWritable(sink));
	}
}
