package cp2016.pagerank.parse;

import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import cp2016.pagerank.common.TitleLinkPair;

public class Spliter extends Partitioner<IntWritable, TitleLinkPair> {

	@Override
	public int getPartition(IntWritable key, TitleLinkPair value, int numReducers) {
		Random gen = new Random();
		return gen.nextInt(numReducers);
	}
	
}
