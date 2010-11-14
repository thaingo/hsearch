package com.bizosys.hsearch.common;

import java.util.Date;

public class HField {
	
	public boolean isIndexable = true;
	public boolean isAnalyzed = true;
	public boolean isStored = true;
	
	public ByteField bfl = null;
	
	
	public HField(String name, String value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, long value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	public HField(String name, byte value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, boolean value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, short value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	public HField(String name, int value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, float value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, double value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, Date value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	public HField(String name, byte[] value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
}
