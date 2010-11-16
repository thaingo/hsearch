package com.bizosys.hsearch.outpipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.filter.FilterMetaAndAcl;
import com.bizosys.hsearch.filter.PreviewFilter;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.L;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
public class CheckMetaInfo_HBase {
	
	public PreviewFilter pf;
	
	public CheckMetaInfo_HBase(QueryContext ctx) {
		PreviewFilter pf = new PreviewFilter();
		FilterMetaAndAcl setting = pf.getFma();

		if ( null != ctx.queryString ) setting.keyword = 
			Storable.putString(ctx.queryString.toLowerCase());  
		if ( null != ctx.state ) setting.state = ctx.state.toBytes();  
		if ( null != ctx.tenant ) setting.tenant = ctx.tenant.toBytes();  
		if ( null != ctx.createdAfter) setting.createdAfter = ctx.createdAfter.longValue();  
		if ( null != ctx.createdBefore) setting.createdBefore = ctx.createdBefore.longValue();  
		if ( null != ctx.modifiedAfter) setting.modifiedAfter = ctx.modifiedAfter.longValue();  
		if ( null != ctx.modifiedBefore) setting.modifiedBefore = ctx.modifiedBefore.longValue();  
	}
	
	public List<DocMetaWeight> filter(Object[] staticL, 
		int  scroll, int pageSize ) throws ApplicationFault {
		
		L.l.debug("BuildPreviewPage_HBase > Call START");
		if ( null == this.pf) return null;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int staticT = staticL.length;
		if ( staticT <= scroll) scroll = 0;
		
		
		/**
		 * Step 1 Identify table, family and column
		 */
		String tableName = IOConstants.TABLE_PREVIEW;
		byte[] familyName = IOConstants.SEARCH_BYTES;
		byte[] colName = IOConstants.META_BYTES;
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		HTableWrapper table = null;
		HBaseFacade facade = null;
		ResultScanner scanner = null;

		List<DocMetaWeight> foundDocs = new ArrayList<DocMetaWeight>();
		try {

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			int totalMatched = 0;
			for (int i=scroll; i< staticT; i++ ) {
				if ( totalMatched > pageSize) break; //Just read enough for the page size

				String id = ((DocWeight) staticL[i]).id;
				Get getter = new Get(id.getBytes());;
				getter.setFilter(this.pf);
				
				byte [] meta = HReader.getScalar( 
					tableName,familyName, colName, id.getBytes());
				if ( null == meta) continue;
				foundDocs.add(new DocMetaWeight(id,meta));
				totalMatched++;
			}
			return foundDocs;
			
		} catch ( IOException ex) {
			L.l.fatal("CheckMetaInfo_HBase:", ex);
			throw new ApplicationFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}	
	}
}