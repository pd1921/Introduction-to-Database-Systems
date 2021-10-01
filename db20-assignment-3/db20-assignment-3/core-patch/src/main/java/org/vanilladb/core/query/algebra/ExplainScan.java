package org.vanilladb.core.query.algebra;

import java.sql.ResultSet;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.Schema;

public class ExplainScan implements Scan {
	private String a;
	private Schema schema;
	private int recs;
	private boolean flag;  // is before first executed

	/**
	 * Creates a product scan having the two underlying scans.
	 * 
	 * @param s1
	 *            the LHS scan
	 * @param s2
	 *            the RHS scan
	 */
	public ExplainScan(Scan s, String a, Schema schema) {
		this.a = a;
		this.schema = schema;
		s.beforeFirst(); // explainscan.beforefirst
		while (s.next()) {
			recs++;
		}
		s.close();
		//System.out.println("3: " + this.a);
		this.a = a + "\nActual #recs: " + recs;
		flag = true;
	}

	/**
	 * Positions the scan before its first record. In other words, the LHS scan
	 * is positioned at its first record, and the RHS scan is positioned before
	 * its first record.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		flag = true;
	}

	/**
	 * Moves the scan to the next record. The method moves to the next RHS
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first RHS record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (flag) {
			flag = false;
			return true;
		} else
			return false;
	}

	/**
	 * Closes both underlying scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		//System.out.println("2: " + a);
		if (fldName.equals("query-plan")) {
			return new VarcharConstant(a);
		} else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}
}
