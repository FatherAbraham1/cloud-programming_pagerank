package cp2016.pagerank.result;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cp2016.pagerank.common.TitleRanksPair;

public class RowComparator extends WritableComparator {

	public RowComparator() {
		super(TitleRanksPair.class, true);
	}

  @Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		TitleRanksPair left = (TitleRanksPair) lhs;
		TitleRanksPair right = (TitleRanksPair) rhs;
		int result = Double.compare(right.getCurrentRank(), left.getCurrentRank());
		if (result != 0) {
			return result;
		} else {
			return left.getTitle().compareTo(right.getTitle());
		}
	}
}
