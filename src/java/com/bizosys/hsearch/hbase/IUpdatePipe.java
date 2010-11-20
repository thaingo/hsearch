package com.bizosys.hsearch.hbase;

public interface IUpdatePipe {

	/**
	 * This pipe process and provide the new updated records
	 * @param family Existing Family
	 * @param name  Existing column Qualifier
	 * @param existingB Existing value
	 * @return
	 */
	byte[] process(byte[] family, byte[] name, byte[] existingB);
}
