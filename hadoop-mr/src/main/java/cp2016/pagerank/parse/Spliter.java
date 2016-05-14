package cp2016.pagerank.parse;

import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Spliter extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numReducers) {
		Random gen = new Random();
		return gen.nextInt(numReducers);
	}
	
}
