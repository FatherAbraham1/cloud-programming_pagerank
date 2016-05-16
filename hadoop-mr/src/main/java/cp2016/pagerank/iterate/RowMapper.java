package cp2016.pagerank.iterate;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

import cp2016.pagerank.common.RanksLinkPair;

public class RowMapper extends Mapper<LongWritable, Text, Text, RanksLinkPair>{

	@Override 
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] pages = value.toString().split("\n");
		for (String page : pages) {
			String[] values = page.split("\t");
			if (values.length == 4) {
				String title = values[0];
				double currentRank = Double.parseDouble(values[2]);
				List<String> links = JSON.parseArray(values[3], String.class);
				if (links.size() > 0) { 
					double avgScore = currentRank / links.size();
					for (String link : links) {
						context.write(new Text(link), new RanksLinkPair(0.0, avgScore, ""));
					}
				}
				
				// save original graph
				context.write(new Text(title), new RanksLinkPair(currentRank, 0.0, values[3]));
			}
		}
	}
}
