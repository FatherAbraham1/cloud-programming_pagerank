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
			String link = kv[0];
			String titlesJSON = kv[1];
			
			List<String> titles = JSON.parseArray(titlesJSON, String.class);
			if (titles.contains(link)) {
				if (titles.size() == 1) {
					context.write(new Text(titles.get(0)), new Text(""));
				} else {
					for (String title : titles) {
						if(!link.equals(link)) {
							context.write(new Text(title), new Text(link));
						}
					}
				}
			}
		}
	}
}
