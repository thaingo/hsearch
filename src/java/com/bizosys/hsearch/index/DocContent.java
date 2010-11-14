package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import org.apache.oneline.ApplicationFault;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.common.StorableList;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;

/**
 * The content of the original document
 * @author bizosys
 *
 */
public class DocContent implements IDimension {

	/**
	 * The content, It could be inline or a URL
	 */
	public List<ByteField> stored =  null;
	public List<ByteField> analyzedIndexed =  null;
	public List<ByteField> nonAnalyzedIndexed =  null;
	
	/**
	 * To which the document has cited
	 */
	public StorableList citationTo =  null;

	/**
	 * From which the document has cited
	 */
	public StorableList citationFrom =  null;
	
	
	public DocContent(HDocument aDoc) {
		if(null == aDoc) return;
		
		this.citationTo = aDoc.citationTo;
		this.citationFrom = aDoc.citationFrom;

		if(null == aDoc.fields) return;
		for (HField fld: aDoc.fields) {
			if ( fld.isStored) {
				if ( null == this.stored) this.stored = new ArrayList<ByteField>(); 
				this.stored.add(fld.bfl);
			} 
			
			if ( fld.isIndexable) {
				if (fld.isAnalyzed) {
					if ( null == this.analyzedIndexed) 
						this.analyzedIndexed = new ArrayList<ByteField>();
					this.analyzedIndexed.add(fld.bfl);
				} else {
					if ( null == this.nonAnalyzedIndexed) 
						this.nonAnalyzedIndexed = new ArrayList<ByteField>(); 
					this.nonAnalyzedIndexed.add(fld.bfl);
				}
			}
		}
	}
	
	public DocContent (List<NVBytes> nvs) throws ApplicationFault {
		
		for (NVBytes nv : nvs) {
			switch(nv.family[0]) {
				case IOConstants.CONTENT_CITATION:
					switch(nv.name[0]) {
						case IOConstants.CONTENT_CITATION_TO:
							this.citationTo = new StorableList(nv.data);
							break;
						case IOConstants.CONTENT_CITATION_FROM:
							this.citationFrom = new StorableList(nv.data);
							break;
					}
				case IOConstants.CONTENT_FIELDS:
					this.stored.add(ByteField.wrap(nv.name, nv.data));
			}
		}
	}	

	public void toNVs(List<NV> nvs) throws ApplicationFault {
		if ( null != this.stored ) {
			for (ByteField bf : this.stored) {
				bf.enableTypeOnToBytes(true);
				nvs.add(new NV(
					IOConstants.CONTENT_FIELDS_BYTES, bf.getName(), new Storable(bf.toBytes())
					)
				);
			}
		}
		
		if ( null != this.citationTo ) nvs.add( new NV(
			IOConstants.CONTENT_CITATION_BYTES,
			IOConstants.CONTENT_CITATION_TO_BYTES, this.citationTo ) );

		if ( null != this.citationFrom ) nvs.add( new NV(
			IOConstants.CONTENT_CITATION_BYTES,
			IOConstants.CONTENT_CITATION_TO_BYTES, this.citationFrom ) );		
	}
	
	/**
	 * Clean up the entire set.
	 */
	public void cleanup() {
		if ( null != this.stored ) this.stored.clear();
		this.citationTo =  null; 
		this.citationFrom =  null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != this.stored ) sb.append("Fields:").append(this.stored.size());
		if ( null != this.citationTo ) sb.append("Citation To :").append(new String(this.citationTo.toString()));
		if ( null != this.citationFrom ) sb.append("Citation From :").append(new String(this.citationFrom.toString()));
		return sb.toString();
	}


}
