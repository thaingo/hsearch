package com.bizosys.hsearch.inpipe;

import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.InvertedIndex;

public class DeleteFromIndexWithCut implements IUpdatePipe {
	short docSerialId = -1;
	public DeleteFromIndexWithCut(short docSerialId) {
		this.docSerialId = docSerialId; 
	}
	
	public byte[] process(byte[] family, byte[] name, byte[] existingB) {
		return InvertedIndex.delete(existingB, docSerialId);
	}

}
