package cp2016.pagerank.filter;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

public class RowMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] records = value.toString().split("\n");
		for (String r : records) {
			String[] kv = r.split("\t");
			String title = kv[0];
			String linksJSON = kv[1];
			
			List<String> links = JSON.parseArray(linksJSON, String.class);
			if (links.contains(title)) {
				for (String link : links) {
					if(!link.equals(title)) {
						context.write(new Text(title), new Text(link));
					}
				}
			}
		}
	}
}
