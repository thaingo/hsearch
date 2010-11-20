package com.bizosys.hsearch.hbase;

import junit.framework.TestCase;

import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

import com.bizosys.ferrari.TestFerrari;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;

public class HReaderTest extends TestCase {

	public static void main(String[] args) throws Exception {
		HReaderTest t = new HReaderTest();
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
        TestFerrari.testRandom(t);
	}
	
	public void testKeyGeneration() throws Exception {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES,IOConstants.NAME_VALUE_BYTES);
		byte[] pk = "BUCKET_COUNTER".getBytes();		
		RecordScalar scalar = new RecordScalar(pk, nv);
		scalar.pk = new Storable(pk);
		if ( ! HReader.exists(IOConstants.TABLE_CONFIG, pk)) {
			HWriter.insertScalar(IOConstants.TABLE_CONFIG, scalar);
		}
		long bucketId = HReader.generateKeys(IOConstants.TABLE_CONFIG,scalar,1);
		System.out.println(bucketId);
	}
}
