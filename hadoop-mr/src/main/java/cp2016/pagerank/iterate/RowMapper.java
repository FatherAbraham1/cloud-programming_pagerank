package cp2016.pagerank.iterate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cp2016.pagerank.common.TitleRankPair;

public class RowMapper extends Mapper<LongWritable, Text, TitleRankPair, Text>{

	@Override 
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
	}
}
