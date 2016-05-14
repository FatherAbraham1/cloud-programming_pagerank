package cp2016.pagerank.filter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cp2016.pagerank.common.TitleRankPair;

public class SortComparator extends WritableComparator {
	public SortComparator() {
		super(TitleRankPair.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		TitleRankPair left = (TitleRankPair) lhs;
		TitleRankPair right = (TitleRankPair) rhs;
		return left.getTitle().compareTo(right.getTitle());
	}
}
