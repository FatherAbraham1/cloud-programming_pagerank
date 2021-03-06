package cp2016.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cp2016.pagerank.common.MapCounter;
import cp2016.pagerank.common.RanksLinkPair;
import cp2016.pagerank.common.TitleRanksPair;

public class App {
	
	private static final double STICKNESS_FACTOR = 0.85;
	private static final double CONVERGENCE_CRITERIA = 0.001;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath = args[0];
		String outputPath = args[1];
		long iter = 0;
		String tmpGraphPath = outputPath + "-tmp-graph-";
		
		long parsingStart = System.currentTimeMillis();
		System.out.println("Start parsing...");
		long recordCount = parse(inputPath, outputPath + "-inverted-graph");
		filter(outputPath + "-inverted-graph", tmpGraphPath + Long.toString(iter), recordCount);
		long parsingEnd = System.currentTimeMillis();
		System.out.println("Done parsing: " + Double.toString((parsingEnd - parsingStart) / 1000.0));
		
		String sinkNodeFile = outputPath + "-sinkNodeFile";
		String diffFile = outputPath + "-diffFile";
		boolean converges = false;
		do {
			System.out.println("iteration #" + Long.toString(iter) + " started");
			long iterStart = System.currentTimeMillis();
			double sinkNodeScore = sinkNodeScore(tmpGraphPath + Long.toString(iter), sinkNodeFile, recordCount);
			System.out.println("Sink node score: " + Double.toString(sinkNodeScore));
			String newPath = tmpGraphPath + Long.toString(iter + 1);
			iterate(tmpGraphPath + Long.toString(iter), newPath, recordCount, sinkNodeScore);
			double diff = diff(newPath, diffFile);
			converges = diff < CONVERGENCE_CRITERIA;
			long iterEnd = System.currentTimeMillis();
			System.out.println("Score updates: " + Double.toString(diff));
			System.out.println("iteration #" + Long.toString(iter) + " ended: " + Double.toString((iterEnd - iterStart) / 1000.0));
			iter += 1;
		} while(!converges);
		
		System.exit(result(tmpGraphPath + Long.toString(iter), outputPath));
	}
	
	private static long parse(String inputFile, String outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");

		Job job = Job.getInstance(config, "XMLParser");
		job.setJarByClass(App.class);

		job.setMapperClass(cp2016.pagerank.parse.RowMapper.class);
		job.setReducerClass(cp2016.pagerank.parse.RowReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(64);
		
		TextInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
		
		return job.getCounters().findCounter(MapCounter.InputRecords).getValue();
	}
	
	private static void filter(String inputPath, String outputPath, long numDocs) throws IOException, IllegalArgumentException, ClassNotFoundException, InterruptedException {
		
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");
    	
    	config.setLong("numberOfTitles", numDocs);
    	
		Job job = Job.getInstance(config, "LinkFilter");
		job.setJarByClass(App.class);
		
		job.setMapperClass(cp2016.pagerank.filter.RowMapper.class);
		job.setReducerClass(cp2016.pagerank.filter.RowReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(TitleRanksPair.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(64);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
		
	}
	
	private static double sinkNodeScore(String inputPath, String outputPath, long numDocs) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "");
    	
    	config.setLong("numberOfTitles", numDocs);
    	config.set("sinkNodeOutputFile", outputPath);
    	
		Job job = Job.getInstance(config, "SinkNodeScoreAggregator");
		job.setJarByClass(App.class);

		job.setMapperClass(cp2016.pagerank.sink.RowMapper.class);
		job.setReducerClass(cp2016.pagerank.sink.RowReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
		TextInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + Long.toString(System.currentTimeMillis())));
		
		job.waitForCompletion(true);
		
		double sinkNodeScore = 0.0;
    	{
    		FileSystem fs = FileSystem.get(config);
    		Path sinkNodeFilePath = new Path(outputPath);
    		FSDataInputStream inStream = fs.open(sinkNodeFilePath);
    		InputStreamReader reader = new InputStreamReader(inStream);
    		BufferedReader br = new BufferedReader(reader);
    		String score = null;
    		score = br.readLine();

    		if (score != null) {
    			sinkNodeScore = Double.parseDouble(score) * STICKNESS_FACTOR;
    		}

    		br.close();
        	reader.close();
        	inStream.close();
    	}

    	return sinkNodeScore;
	}
	
	private static void iterate(String inputPath, String outputPath, long numDocs, double sinkNodeScore) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");

    	config.setDouble("stickness", STICKNESS_FACTOR);

    	config.setDouble("constantFactor", (1.0 - STICKNESS_FACTOR) / numDocs + sinkNodeScore);

		Job job = Job.getInstance(config, "PageRankIterator");
		job.setJarByClass(App.class);

		job.setMapperClass(cp2016.pagerank.iterate.RowMapper.class);
		job.setReducerClass(cp2016.pagerank.iterate.RowReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RanksLinkPair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(64);

		TextInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}
	
	private static double diff(String inputPath, String outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");
    	config.set("diffOutputFile", outputPath);
    	
		Job job = Job.getInstance(config, "PageDiffCalculator");
		job.setJarByClass(App.class);

		job.setMapperClass(cp2016.pagerank.diff.RowMapper.class);
		job.setReducerClass(cp2016.pagerank.diff.RowReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(64);

		TextInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(inputPath + "@" + Long.toString(System.currentTimeMillis())));

		job.waitForCompletion(true);
		
		
		double diff = 0.0;
    	{
    		FileSystem fs = FileSystem.get(config);
    		Path diffFilePath = new Path(outputPath);
    		FSDataInputStream inStream = fs.open(diffFilePath);
    		InputStreamReader reader = new InputStreamReader(inStream);
    		BufferedReader br = new BufferedReader(reader);
    		String score = null;
    		score = br.readLine();

    		if (score != null) {
    			diff = Double.parseDouble(score);
    		}

    		br.close();
        	reader.close();
        	inStream.close();
    	}
		
		
		return diff;
	}
	
	private static int result(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
    	config.set("mapreduce.output.textoutputformat.separator", "\t");
    	
		Job job = Job.getInstance(config, "Sorter");
		job.setJarByClass(App.class);

		job.setMapperClass(cp2016.pagerank.result.RowMapper.class);
		job.setSortComparatorClass(cp2016.pagerank.result.RowComparator.class);
		
		job.setReducerClass(cp2016.pagerank.result.RowReducer.class);
		
		job.setMapOutputKeyClass(TitleRanksPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setNumReduceTasks(1);
		
		TextInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
