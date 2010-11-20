package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.services.batch.BatchTask;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;
import com.bizosys.hsearch.util.RecordScalar;

public class TermWeight implements BatchTask {
	
	public static String TERM_SIGHT = "TERM_SIGHT";
	public static byte[] TERM_SIGHT_BYTES = TERM_SIGHT.getBytes();
	
	public static TermWeight instance = null;
	public static TermWeight getInstance() throws ApplicationFault {
		if ( null != instance ) return instance;
		synchronized (TermWeight.class) {
			if ( null != instance ) return instance;
			instance = new TermWeight();
			return instance;
		}
	}
	
	public Map<Character, Byte> weights = new HashMap<Character, Byte>();
	
	public TermWeight() throws ApplicationFault {
		
		this.process();
	}
	
	public int getWeight(String sight) throws ApplicationFault {
		if (this.weights.containsKey(sight))
			return this.weights.get(sight);
		
		throw new ApplicationFault("Configuration Error : Sightting weight for " + sight + " is not assigned.");
	}
	
	public void persist() throws IOException {
		
		try {
			int totalSize = 0;
			if ( 0 == totalSize ) return;

			totalSize = weights.size() * 2;
			byte[] bytes = new byte[totalSize];
			
			int pos = 0;
			for (char type : weights.keySet()) {
				bytes[pos++] = (byte) type;
				bytes[pos++] = weights.get(type);
			}
			NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES, new Storable(bytes));
			Record record = new Record(new Storable(TERM_SIGHT_BYTES), nv);
			HWriter.update(IOConstants.TABLE_CONFIG, record);
		} catch (IOException e) {
			e.printStackTrace(System.out);
			throw e;
		}
	}

	public String getJobName() {
		return "TermWeight";
	}

	public Object process() throws ApplicationFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		
		RecordScalar scalar = new RecordScalar(TERM_SIGHT_BYTES, nv);
		HReader.getScalar(IOConstants.TABLE_CONFIG, scalar);
		if ( null == nv.data) return 1;
		
		byte[] bytes = nv.data.toBytes();
		int total = bytes.length;
		
		int pos = 0;
		
		Map<Character, Byte> newTypes = new HashMap<Character, Byte>();
		while ( pos < total) {
			char sight = (char) bytes[pos++];
			byte weightCode = bytes[pos++];
			newTypes.put(sight, weightCode);
		}
		Map<Character, Byte> temp = this.weights;
		this.weights = newTypes;
		temp.clear();
		temp = null;
		
		/**
		 * Populate with default values
		 */
		if ( 0 == this.weights.size()) {
			weights.put(Term.TERMLOC_URL, (byte) 100);
			weights.put(Term.TERMLOC_SUBJECT, (byte) 90);
			weights.put(Term.TERMLOC_XML, (byte) 75);
			weights.put(Term.TERMLOC_BODY, (byte) 60);
			weights.put(Term.TERMLOC_META, (byte) 50);
			weights.put(Term.TERMLOC_KEYWORD, (byte) 35);
		}
		return 0;
	}

	public void setJobName(String jobName) {
	}
	
	public static void main(String[] args) throws Exception {
		TermWeight weight = new TermWeight();
		weight.weights.put(Term.TERMLOC_XML, (byte) 100);
		weight.persist();
	}
	
}
