
package com.vg.hbase.operations.base;

import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author skrishnanunni
 * @date 07-Feb-2013 : 4:22:59 PM
 */
public class LongComparator extends WritableByteArrayComparable {

	/**
	 * Instantiates a new long comparator.
	 */
	public LongComparator() {

		super();
	}

	/**
	 * Instantiates a new long comparator.
	 * 
	 * @param value
	 *            the value
	 */
	public LongComparator(byte[] value) {

		super(value);
	}

	/**
	 * Instantiates a new long comparator.
	 * 
	 * @param val
	 *            the val
	 */
	public LongComparator(Long val) {

		super(Bytes.toBytes(val));
	}

	/**
	 * @see org.apache.hadoop.hbase.filter.WritableByteArrayComparable#compareTo(byte[])
	 */
	@Override
	public int compareTo(byte[] other) {

		Long o = Bytes.toLong(other);
		Long t = Bytes.toLong(this.getValue());

		if (t == o) {
			return 0;
		}

		if (t < o) {
			return -1;
		}

		return 1;
	}
}