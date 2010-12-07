package com.bizosys.hsearch.common;

import java.util.Date;

import com.bizosys.oneline.SystemFault;

/**
 * Serializable Field for document content.
 * If using embedded in embedded mode use <code>HField</code> class.
 * @see HField
 */
public class SField implements Field{

	/**
	 *	Indexable field 
	 */
	private boolean index = true;
	
	/**
	 * Is analyzable.
	 */
	private boolean analyze = true;
	
	/**
	 * Requires storing
	 */
	private boolean store = true;
	
	/**
	 * Field type. @See <code>Storable</code> for allowed types
	 */
	public byte type = Storable.BYTE_UNKNOWN;
	
	/**
	 * Field name
	 */
	public String name;
	
	/**
	 * Field Value
	 */
	public String value;
	
	
	private ByteField bfl = null;
	
	/**
	 * Default Constructor
	 * @param index	Is Indexable
	 * @param analyze	Is Anlyzed
	 * @param store	Should Store
	 * @param type	Data Type @See <code>Storable</code> for allowed types
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public SField(boolean index,boolean analyze,boolean store,
		byte type, String name, String value) {
		
		this.index = index;
		this.analyze = analyze;
		this.store = store;
		this.type = type;
		this.name = name;
		this.value = value;
	}
	
	/**
	 * @return ByteField	The ByteField representation of name-value 
	 */
	public ByteField getByteField() throws SystemFault {
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
				throw new SystemFault("Unknown data type :" + type);
		}
		return bfl;
	}

	/**
	 * Specifies whether a field should be analyzed for extracting words.
	 * @return	True if requires analysis
	 */
	public boolean isAnalyze() {
		return this.analyze;
	}

	/**
	 * Specifies whether a field should be indexed.
	 * @return	True is Indexable
	 */
	public boolean isIndexable() {
		return this.index;
	}

	/**
	 * Specifies whether a field should be stored.
	 * @return	True if storing
	 */
	public boolean isStore() {
		return this.store;
	}
}
