package com.bizosys.hsearch.hbase;

import com.bizosys.hsearch.common.IStorable;
import com.bizosys.hsearch.common.Storable;

public class NV {

	public IStorable family = null;
	public IStorable name = null;
	public IStorable data = null;

	public NV(String family, String name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	public NV(byte[] family, byte[] name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	public NV(byte[] family, byte[] name, IStorable data) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}
	
	public NV(String family, String name, IStorable data ) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}

	public NV(IStorable family, IStorable name, IStorable data ) {
		this.family = family;
		this.name = name;
		this.data = data;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder(50);
		sb.append("  F:[").append(new String(family.toBytes())).
		append("] N:[").append(new String(name.toBytes())).
		append("] D:").append(data.toString());
		return sb.toString();
	}
}
