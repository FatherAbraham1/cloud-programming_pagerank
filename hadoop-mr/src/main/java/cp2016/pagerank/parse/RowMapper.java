package cp2016.pagerank.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

public class RowMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final Pattern titlePattern = Pattern.compile("<title>.+</title>");
	private final Pattern textPattern = Pattern.compile("<text>.+</text>");
	private final Pattern linkPattern = Pattern.compile("\\[\\[.+\\]\\]");
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println(value.toString());
		String[] vals = value.toString().split("\n");
		for (String val : vals) {
			if (val == null || val.isEmpty()) {
				continue;
			} else {
				String title = null;
				Matcher titleMatcher = titlePattern.matcher(val);
				if (titleMatcher.find()) {
					title = titleMatcher.group();
					title = title.substring(8, title.length() - 8);
					title = StringEscapeUtils.unescapeXml(title);
					title = WordUtils.capitalize(title);
				}
				
				String text = null;
				Matcher textMatcher = textPattern.matcher(val);
				if (textMatcher.find()) {
					text = textMatcher.group();
					text = text.substring(7, text.length() - 7);
				}
				
				if (title == null || text == null) {
					continue;
				}
				
				List<String> links = parseLinks(text);
				context.write(new Text(title), new Text(JSON.toJSONString(links)));
			}
		}
	}
	
	private List<String> parseLinks(String content) {
		List<String> links = new ArrayList<>();
		Matcher linkMatcher = linkPattern.matcher(content);
		while (linkMatcher.find()) {
			String linkContent = linkMatcher.group();
			linkContent = linkContent.substring(2, linkContent.length() - 2);
			{
				String[] tmp = linkContent.split("\\|");
				if (tmp.length > 0) {
					linkContent = tmp[0];
				}
			}
			
			{
				String[] tmp = linkContent.split("#");
				if (tmp.length > 0) {
					linkContent = tmp[0];
				}
			}
			
			links.add(WordUtils.capitalize(StringEscapeUtils.unescapeXml(linkContent)));
			
		}
		return links;
	}
}
