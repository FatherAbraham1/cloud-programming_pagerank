package cp2016.pagerank.iterate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cp2016.pagerank.common.RanksLinkPair;

public class RowReducer extends Reducer<Text, RanksLinkPair, Text, Text> {
	@Override
	public void reduce(Text key,
			Iterable<RanksLinkPair> values, Context context)
			throws IOException, InterruptedException {
		double constantFactor = context.getConfiguration().getDouble("constantFactor", 0.0);
		if (constantFactor <= 0.0) {
			throw new RuntimeException("add 0.0?");
		}
		
		double newScore = 0.0;
		double oldScore = 0.0;
		String link = null;
		for (RanksLinkPair p : values) {
			oldScore += p.getPreviousRank();
			newScore += p.getCurrentRank();
			if (link == null) {
				String linkString = p.getLinksJSON();
				if (!linkString.isEmpty()) {
					link = linkString;
				}
			}
		}
		double stickness = context.getConfiguration().getDouble("stickness", 0.0);
		newScore *= stickness;
		newScore += constantFactor;
		
		context.write(key, new Text(String.join("\t", Double.toString(oldScore), Double.toString(newScore), link)));
	}
}
