package com.bizosys.hsearch.common;

import java.util.Date;

import com.bizosys.oneline.ApplicationFault;

public class SField implements Field{
	public boolean index = true;
	public boolean analyze = true;
	public boolean store = true;
	public byte type = Storable.BYTE_UNKNOWN;
	public String name;
	public String value;
	
	private ByteField bfl = null;
	
	public SField(boolean index,boolean analyze,boolean store,
		byte type, String name, String value) {
		
		this.index = index;
		this.analyze = analyze;
		this.store = store;
		this.type = type;
		this.name = name;
		this.value = value;
	}
	
	public ByteField getByteField() throws ApplicationFault {
		if ( null != bfl) return bfl;
		switch (type) {
			case Storable.BYTE_BOOLEAN:
				bfl = new ByteField(name,new Boolean(value));
				break;
			case Storable.BYTE_BYTE:
				bfl = new ByteField(name,new Byte(value));
				break;
			case Storable.BYTE_CHAR:
				bfl = new ByteField(name,value.charAt(0));
				break;
			case Storable.BYTE_DATE:
				bfl = new ByteField(name,new Date(new Long(value)));
				break;
			case Storable.BYTE_DOUBLE:
				bfl = new ByteField(name,new Double(value));
				break;
			case Storable.BYTE_FLOAT:
				bfl = new ByteField(name,new Float(value));
				break;
			case Storable.BYTE_INT:
				bfl = new ByteField(name,new Integer(value));
				break;
			case Storable.BYTE_LONG:
				bfl = new ByteField(name,new Long(value));
				break;
			case Storable.BYTE_SHORT:
				bfl = new ByteField(name,new Short(value));
				break;
			case Storable.BYTE_STRING:
				bfl = new ByteField(name,value);
				break;
			default:
				throw new ApplicationFault("Unknown data type :" + type);
		}
		return bfl;
	}

	public boolean isAnalyze() {
		return this.analyze;
	}

	public boolean isIndexable() {
		return this.index;
	}

	public boolean isStore() {
		return this.store;
	}
}
