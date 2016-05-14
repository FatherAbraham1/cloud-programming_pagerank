package cp2016.pagerank.iterate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.ScoreLinkPair;

public class RowReducer extends Reducer<Text, ScoreLinkPair, Text, Text> {
	@Override
	public void reduce(Text key,
			Iterable<ScoreLinkPair> values, Context context)
			throws IOException, InterruptedException {
		double constantFactor = context.getConfiguration().getDouble("constantFactor", 0.0);
		if (constantFactor <= 0.0) {
			throw new RuntimeException("add 0.0?");
		}
		
		double score = 0.0;
		String link = null;
		for (ScoreLinkPair p : values) {
			score += p.getScore();
			if (link == null) {
				String linkString = p.getLinksJSON();
				if (!linkString.isEmpty()) {
					link = linkString;
				}
			}
		}
		
		score += constantFactor;
		
		context.write(key, new Text(Double.toString(score) + "\t" + link));
	}
}
