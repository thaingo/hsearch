package com.bizosys.hsearch.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class StorableList implements List {

	List<byte[]> container = null;
	
	public StorableList() {
		container = new ArrayList<byte[]>();
	}
	
	public StorableList(int size) {
		container =  new ArrayList<byte[]>(size);
	}

	public StorableList(byte[] inputBytes) {
		this(inputBytes, 0);
	}
	
	public StorableList(byte[] inputBytes, int seek) {
		container = new ArrayList<byte[]>();
		
		if ( null != inputBytes) {
			int totalBytes = inputBytes.length;
			while ( seek < totalBytes ) {
				int contentSize = 0;  //The content size is in short
				for (int i = 0; i < 2 ; i++) {
					contentSize = (contentSize << 8) + (inputBytes[seek + i] & 0xff);
				}
				seek = seek + 2;
				byte[] contentBytes = new byte[contentSize];
				for (int i = 0; i < contentSize ; i++) {
					contentBytes[i] = (inputBytes[seek + i]);
				}
				seek = seek + contentSize;
				container.add(contentBytes);
			}
		}
		
	}	
	
	public boolean add(Object storable) {
		byte[] bytes = (byte[])storable; 
		container.add(bytes); 
		return true;
	}

	public void add(int pos, Object storable) {
		byte[] bytes = (byte[])storable; 
		container.add(pos, bytes); 
	}

	public boolean addAll(Collection colStorable) {
		for (Object storable : colStorable) {
			byte[] bytes = (byte[])storable; 
			container.add(bytes); 
		}
		return true;
	}

	public void clear() {
		this.container.clear();
	}

	public Object get(int arg0) {
		return this.container.get(arg0);
	}

	public boolean addAll(int arg0, Collection arg1) {
		return false;
	}

	public boolean contains(Object arg0) {
		return false;
	}

	public boolean containsAll(Collection arg0) {
		return false;
	}

	public int indexOf(Object arg0) {
		return 0;
	}

	public boolean isEmpty() {
		return this.container.isEmpty();
	}

	public Iterator iterator() {
		return this.container.iterator();
	}

	public int lastIndexOf(Object arg0) {
		return 0;
	}

	public ListIterator listIterator() {
		return null;
	}

	public ListIterator listIterator(int arg0) {
		return null;
	}

	public boolean remove(Object arg0) {
		return false;
	}

	public Object remove(int arg0) {
		container.remove(arg0); 
		return null;
	}

	public boolean removeAll(Collection arg0) {
		return false;
	}

	public boolean retainAll(Collection arg0) {
		return false;
	}

	public Object set(int arg0, Object arg1) {
		return null;
	}

	public int size() {
		return this.container.size();
	}

	public List subList(int arg0, int arg1) {
		return null;
	}

	public Object[] toArray() {
		return null;
	}

	@SuppressWarnings("unchecked")
	public Object[] toArray(Object[] arg0) {
		return null;
	}
	
////// ********************** //////////
	public byte[] toBytes() {
		byte[] outputBytes = null;
		
		if ( null != container) {
			int totalBytes = 0;
			for (byte[] bytes : container) {
				totalBytes = totalBytes + 2 + bytes.length ; //2 is added as size
			}
			
			outputBytes = new byte[totalBytes];
			int seek = 0;
			for (byte[] bytes : container) {

				short byteSize = (short) bytes.length;
				outputBytes[seek++] = (byte)(byteSize >> 8 & 0xff); 
				outputBytes[seek++] = (byte)(byteSize & 0xff) ;
				
				for (byte b : bytes) {
					outputBytes[seek++] = b;
				}
			}
		}
		return outputBytes;
	}
}