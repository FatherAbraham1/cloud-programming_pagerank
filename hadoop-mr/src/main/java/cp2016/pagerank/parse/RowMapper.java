package cp2016.pagerank.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;

import cp2016.pagerank.common.TitleLinkPair;

public class RowMapper extends Mapper<LongWritable, Text, IntWritable, TitleLinkPair> {

	private final Pattern titlePattern = Pattern.compile("<title>.+</title>");
	private final Pattern textPattern = Pattern.compile("<text xml:space=\"preserve\">.+</text>");
	private final Pattern linkPattern = Pattern.compile("\\[\\[[^\\]]+\\]\\]");

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] vals = value.toString().split("\n");
		for (String val : vals) {
			if (val == null || val.isEmpty()) {
				continue;
			} else {
				String title = null;
				Matcher titleMatcher = titlePattern.matcher(val);
				if (titleMatcher.find()) {
					title = titleMatcher.group();
					title = title.substring(7, title.length() - 8);
					title = StringEscapeUtils.unescapeXml(title);
					title = capString(title);
				}

				String text = null;
				Matcher textMatcher = textPattern.matcher(val);
				if (textMatcher.find()) {
					text = textMatcher.group();
					text = text.substring(27, text.length() - 7);
					text = StringEscapeUtils.unescapeXml(text);
				}

				if (title == null) {
					continue;
				}

				List<String> links = new ArrayList<>();
				if (text != null) {
					links = parseLinks(text);
				}

				context.write(new IntWritable(0), new TitleLinkPair(title, JSON.toJSONString(links)));
			}
		}
	}

  private String capString(String input) {
    if (input.length() == 0) {
      return input;
    } else if (input.length() > 1) {
      return input.substring(0, 1).toUpperCase() + input.substring(1);
    } else {
      return input.toUpperCase();
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

			String finalLink = capString(StringEscapeUtils.unescapeXml(linkContent).trim());
			if (!finalLink.isEmpty()){
				links.add(finalLink);
			}

		}
		return links;
	}
}
