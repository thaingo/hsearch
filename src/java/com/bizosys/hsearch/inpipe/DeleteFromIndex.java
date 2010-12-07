/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.inpipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.common.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.schema.EnglishMap;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Delete terms from the inverted index.
 * @author karan
 *
 */
public class DeleteFromIndex implements PipeIn {

	List<Doc> documents = new ArrayList<Doc>();
	
	public DeleteFromIndex() {
	}
	
	public DeleteFromIndex(int docMergeFactor) {
	}

	public boolean visit(Object objDoc) throws ApplicationFault, SystemFault {
		if ( null == objDoc) return false;
		if ( null == documents) documents = new ArrayList<Doc>();
		documents.add((Doc)objDoc);
		return true;
	}

	/**
	 * Cuts out section of docpositions which are in the removal list.
	 */
	public boolean commit() throws ApplicationFault, SystemFault {
		Doc curDoc = null; 
		try {
			for (Doc aDoc : documents) {
				curDoc = aDoc;
				IUpdatePipe pipe = new DeleteFromIndexWithCut(aDoc.docSerialId);
				byte[] pk = Storable.putLong(aDoc.bucketId);
				if ( null == curDoc.terms.all) continue;
				Map<Character,StringBuilder> tables = new HashMap<Character,StringBuilder>();
				EnglishMap map = new EnglishMap();
				
				/**
				 * Build table and family based on terms
				 */
				for (Term aTerm : curDoc.terms.all) {
					char table = map.getTableName(aTerm.term);
					char family = map.getColumnFamily(aTerm.term);
					//char col = map.getColumn(aTerm.term);
					if ( tables.containsKey(table)) {
						StringBuilder sb = tables.get(table); 
						if ( -1 == sb.toString().indexOf(family) ) sb.append(family); 
					} else {
						StringBuilder sb = new StringBuilder();
						sb.append(family);
						tables.put(table, sb);
					}
				}
				
				for (Character c : tables.keySet()) {
					String t = c.toString();
					String strFamilies = tables.get(c).toString();
					char[] charFamilies = strFamilies.toCharArray();
					byte[][] families = new byte[charFamilies.length][];
					for ( int i=0; i<charFamilies.length; i++) {
						families[i] = new byte[] { (byte) charFamilies[i]};
					}
					if ( InpipeLog.l.isDebugEnabled() ) 
						InpipeLog.l.debug("Deleting table " + t + " families " + strFamilies);
					HWriter.update(t, pk, pipe, families);
					
					//Delete the mapping too..
					IdMapping.purge(aDoc.bucketId, aDoc.docSerialId);			
				}
			}
			
			return true;
		} catch (Exception ex) {
			if ( null != curDoc) throw new SystemFault(curDoc.toString(), ex);
			else throw new SystemFault(ex);
		}
	}

	public boolean init(Configuration conf){
		return true;
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "DeleteFromIndex";
	}
}
