package com.bizosys.hsearch.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.hbase.NV;

/**
 * Term Columns keep the term and list of documents containing that term
 * @author karan
 *
 */
public class TermColumns implements IDimension {
	
	/**
	 * Each column containing the column and the list
	 * Map<keyword,Termlist>
	 */
	public char family;
	public Map<Character, TermList> columns = null;
	
	public TermColumns(char columnFamily) {
		this.family = columnFamily;
	}
	
	/**
	 * Add a keyword. Repetition are taken care
	 * @param aTerm
	 */
	public void add(Character col, Term aTerm) {
		if ( null == aTerm) return;

		if ( null == columns) {
			columns = new HashMap<Character, TermList> ();
			TermList tl = new TermList();
			tl.add(aTerm);
			columns.put(col, tl);
			return;
		}
		
		if ( columns.containsKey(col)) {
			columns.get(col).add(aTerm);
		} else {
			TermList tl = new TermList();
			tl.add(aTerm);
			columns.put(col, tl);
		}
	}
	
	public void add(TermColumns otherCols) {
		if ( null == otherCols) return;
		if ( null == otherCols.columns) return;
		for (char col: otherCols.columns.keySet()) {
			TermList otherTerms = otherCols.columns.get(col);
			if ( this.columns.containsKey(col)) {
				this.columns.get(col).add(otherTerms);
			} else {
				this.columns.put(col, otherTerms);
			}
		}
	}

	
	/**
	 * The given document id will be applied to 
	 * @param position
	 */
	public void assignDocPos(int position) {
		if ( null == this.columns) return;
		short pos = (short) position;
		for (TermList termL : this.columns.values()) {
			termL.assignDocPos(pos);
		}
	}
	
	/**
	 * Serialize this
	 */
	public void toNVs(List<NV> nvs) throws ApplicationFault {
		if ( null == columns) return;
		String strFamily = new String(new char[]{this.family});
		for (char col: columns.keySet()) {
			String strCol = new String(new char[]{col});
			nvs.add(new NV(strFamily, strCol, columns.get(col))  );
		}
	}
	
	public void cleanup() {
		if ( null != columns) columns.clear();
		columns = null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nTermColumn: ");
		if ( null == this.columns) {
			sb.append("None");
			return sb.toString();
		}
		
		for ( char col: this.columns.keySet()) {
			sb.append("\nHash ").append(col).append(' ');
			sb.append(this.columns.get(col).toString());
		}
		return sb.toString();
	}

}
