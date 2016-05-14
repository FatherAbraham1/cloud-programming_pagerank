package cp2016.pagerank.iterate;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

import cp2016.pagerank.common.ScoreLinkPair;

public class RowMapper extends Mapper<LongWritable, Text, Text, ScoreLinkPair>{

	@Override 
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] pages = value.toString().split("\n");
		for (String page : pages) {
			String[] values = page.split("\t");
			if (values.length == 3) {
				String title = values[0];
				List<String> links = JSON.parseArray(values[2], String.class);
				if (links.size() > 0) { 
					double avgScore = Double.parseDouble(values[1]) / links.size();
					for (String link : links) {
						context.write(new Text(link), new ScoreLinkPair(avgScore, ""));
					}
				}
				
				// save original graph
				context.write(new Text(title), new ScoreLinkPair(0.0, values[2]));
			}
		}
	}
}
