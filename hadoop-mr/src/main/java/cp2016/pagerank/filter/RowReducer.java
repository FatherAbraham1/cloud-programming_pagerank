package cp2016.pagerank.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;

import cp2016.pagerank.common.TitleRankPair;

public class RowReducer extends Reducer<Text, Text, TitleRankPair, Text> {
	@Override
	public void reduce(Text key,
			Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		long numberOfTitles = config.getLong("numberOfTitles", 0);
		
		if (numberOfTitles == 0) {
			throw new RuntimeException("Number of titles is 0!");
		}
		
		List<String> links = new ArrayList<>();
		for (Text val : values) {
			String valString = val.toString();
			if (!valString.isEmpty()){
				links.add(val.toString());
			}
		}
		
		context.write(new TitleRankPair(key.toString(), 1.0 / numberOfTitles),  new Text(JSON.toJSONString(links)));
	}
}
