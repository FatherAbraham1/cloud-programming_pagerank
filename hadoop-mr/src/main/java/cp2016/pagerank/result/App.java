package cp2016.pagerank.result;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cp2016.pagerank.common.TitleLinkPair;
import cp2016.pagerank.common.TitleRankPair;

public class App {
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
    	Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");
    	
		Job job = Job.getInstance(config, "Sorter");
		job.setJarByClass(App.class);

		job.setMapperClass(RowMapper.class);
		job.setSortComparatorClass(RowComparator.class);
		
		job.setReducerClass(RowReducer.class);
		
		job.setMapOutputKeyClass(TitleRankPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setNumReduceTasks(1);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
