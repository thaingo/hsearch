package com.bizosys.hsearch.hbase;

public class NVBytes {

	public byte[] family = null;
	public byte[] name = null;
	public byte[] data = null;

	public NVBytes(byte[] family, byte[] name) {
		this.family = family;
		this.name = name;
	}
	
	public NVBytes(byte[] family, byte[] name, byte[] data) {
		this.family = family;
		this.name = name;
		this.data = data;
	}

	public String toString() {
		return new String(family) + ":" + new String(name) +  ":" + new String(data);
	}
}
