package cp2016.pagerank.filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cp2016.pagerank.common.TitleLinkPair;
import cp2016.pagerank.common.TitleRankPair;

public class App {
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
    	Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");
    	
    	importTitles(config);
    	
		Job job = Job.getInstance(config, "LinkFilter");
		job.setJarByClass(App.class);

		job.setMapperClass(RowMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TitleLinkPair.class);
		
		job.setOutputKeyClass(TitleRankPair.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    private static void importTitles(Configuration config) throws IOException {
    	Path path = new Path("tmp/titles");
		FileSystem fs = FileSystem.get(config);
		FSDataInputStream inStream = fs.open(path);
		InputStreamReader inReader = new InputStreamReader(inStream);
		BufferedReader br = new BufferedReader(inReader);
		
		long count = 0;
		String title = null;
		while((title = br.readLine()) != null) {
			if (title.isEmpty()) {
				continue;
			}
			count += 1;
			config.setBoolean("👉" + title, true);
		}
		br.close();
		inReader.close();
		inStream.close();
		
		config.setLong("numberOfTitles", count);
    }
}
