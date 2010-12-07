package com.bizosys.hsearch.common;

import com.bizosys.oneline.SystemFault;

/**
 * A field is a section of a Document Content.
 * Each field has two parts, a name and a value. 
 * Values may be free text, provided as a String or as any Java data type 
 * or serializable atomic object implementing IStorable interface. 
 */
public interface Field {
	/**
	 * Specifies whether a field should be indexed.
	 * @return	True is Indexable
	 */
	boolean isIndexable();
	
	/**
	 * Specifies whether a field should be analyzed for extracting words.
	 * @return	True if requires analysis
	 */
	boolean isAnalyze();
	
	/**
	 * Specifies whether a field should be stored.
	 * @return	True if storing
	 */
	boolean isStore();
	
	/**
	 * Get the name and value of the field
	 * @return	Returns the <code>ByteField</code>
	 * @throws SystemFault	Any issue on parsing throws SystemFault
	 */
	ByteField getByteField() throws SystemFault;
}