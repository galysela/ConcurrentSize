package algorithms.iterator;

import java.util.concurrent.atomic.AtomicReference;

enum ReportType {add, remove};

class ReportItem {
	Object node;
	ReportType t;
	AtomicReference<ReportItem> next;
	int key;
	int id;

	public ReportItem(Object node, ReportType t, int key) {
		this.node = node;
		this.t = t;
		this.next = new AtomicReference<ReportItem>(null);
		this.key = key;
	}
}

class CompactReportItem implements Comparable{
	Object node;
	ReportType t;
	int key;
	int id;
	
	public CompactReportItem(Object node, ReportType t, int key) {
		this.node = node;
		this.t = t;
		this.key = key;
	}

	public int compareTo(Object arg0) {
		CompactReportItem other = (CompactReportItem)arg0;
		if (this.key != other.key)
			return this.key - other.key;
		//if (this.id != other.id)
		//	return this.id - other.id;
		if (this.node != other.node)
			return this.node.hashCode() - other.node.hashCode();
		return this.t.ordinal() - other.t.ordinal();
	}
}