package com.bizosys.hsearch.inpipe.util;

import java.io.Reader;

public class ReaderType {
	public Reader reader = null;
	public Character docSection = null;
	public String type = null;

	public ReaderType(Character docSection, String type, Reader reader ) {
		this.docSection = docSection;
		this.type = type;
		this.reader = reader;
	}

}
