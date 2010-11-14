package com.bizosys.hsearch.index;

public class BucketIsFullException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public long currentCount = 0;
	
	public BucketIsFullException(long currentCount) {
		this.currentCount = currentCount;
	}

	public BucketIsFullException(String arg0) {
		super(arg0);
	}

	public BucketIsFullException(Throwable arg0) {
		super(arg0);
	}

	public BucketIsFullException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}
	
	@Override
	public String getMessage() {
		return "Crossed limit till :" + currentCount;
	}
}