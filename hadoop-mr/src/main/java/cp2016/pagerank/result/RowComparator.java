package cp2016.pagerank.result;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cp2016.pagerank.common.TitleRankPair;

public class RowComparator extends WritableComparator {

	public RowComparator() {
		super(TitleRankPair.class, true);
	}

  @Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		TitleRankPair left = (TitleRankPair) lhs;
		TitleRankPair right = (TitleRankPair) rhs;
		int result = Double.compare(left.getRank(), right.getRank());
		if (result != 0) {
			return result;
		} else {
			return left.getTitle().compareTo(right.getTitle());
		}
	}
}
