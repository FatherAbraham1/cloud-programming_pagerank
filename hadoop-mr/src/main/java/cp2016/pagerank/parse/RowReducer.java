package cp2016.pagerank.parse;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;

public class RowReducer extends Reducer<Text, Text, Text, Text> {
	
	
	@Override
	public void reduce(Text key,
			Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.getCounter("", "");
		List<String> links = new ArrayList<>();
		for (Text link : values) {
			links.add(link.toString());
		}
		context.write(key, new Text(JSON.toJSONString(links)));
	}
}
