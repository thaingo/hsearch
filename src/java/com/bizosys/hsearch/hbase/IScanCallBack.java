package com.bizosys.hsearch.hbase;

public interface IScanCallBack {
	void process(byte[] storedBytes);
}
