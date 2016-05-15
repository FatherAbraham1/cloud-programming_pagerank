package cp2016.pagerank.iterate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cp2016.pagerank.common.ScoreLinkPair;

public class App {
    public static void main( String[] args ) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
    	Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");

    	FileSystem fs = FileSystem.get(config);
    	long count = fs.getContentSummary(new Path("tmp/titles")).getFileCount();

    	double stickness = 0.85;
    	config.setDouble("stickness", stickness);

    	double sinkNodeScore = 0.0;
    	{
    		Path sinkNodeFilePath = new Path("tmp/sinkNodeScore");
    		FSDataInputStream inStream = fs.open(sinkNodeFilePath);
    		InputStreamReader reader = new InputStreamReader(inStream);
    		BufferedReader br = new BufferedReader(reader);
    		String score = null;
    		score = br.readLine();

    		if (score != null) {
    			sinkNodeScore = Double.parseDouble(score) * stickness;
    		}

    		br.close();
        	reader.close();
        	inStream.close();
    	}

    	config.setDouble("constantFactor", (1.0 - stickness) / count + sinkNodeScore);

		Job job = Job.getInstance(config, "PageRankIterator");
		job.setJarByClass(App.class);

		job.setMapperClass(RowMapper.class);
		job.setReducerClass(RowReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScoreLinkPair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(64);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
