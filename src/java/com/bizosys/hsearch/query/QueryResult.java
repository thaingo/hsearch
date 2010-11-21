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
package com.bizosys.hsearch.query;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import com.bizosys.oneline.ApplicationFault;


public class QueryResult {
	
	public Object[] sortedStaticWeights = null; //Object = DocWeight
	public Object[] sortedDynamicWeights = null; //DocMetaWeight + weight is adjusted
	public Object[] teasers = null; //DocTeaserWeight + weight is adjusted
	
	public void toXml(Writer writer) throws ApplicationFault{
		if ( null == teasers) return;
		
		try {
			writer.append("<list>");
			int docIndex = 0;
			for (Object teaserO : this.teasers) {
				writer.append("<doc>");
				writer.append("<index>" + docIndex + "</index>");
				DocTeaserWeight dtw = (DocTeaserWeight) teaserO;
				dtw.toXml(writer);
				writer.append("</doc>");
			}
			writer.append("</list>");
		} catch (IOException e) {
			L.l.error("Error in preparing result output.", e);
			throw new ApplicationFault("QueryResult::toXml", e);
		}

	}
	
	@Override
	public String toString() {
		Writer spw = new StringWriter();
		
		try {
			if ( null == this.sortedStaticWeights) {
				spw.write("\nSorted Static Weights = 0 ");
			} else {
				spw.write("\nSorted Static Weights : " + this.sortedStaticWeights.length);
			}
			
			if ( null == this.sortedDynamicWeights) {
				spw.write("\nSorted Dynamic Weights = 0");
			} else {
				spw.write("\nSorted Dynamic Weights : " + this.sortedDynamicWeights.length);
			}

			toXml(spw);
			return spw.toString();
		} catch (Exception ex) {
			return ex.getMessage();
		}
	}
	
	public void cleanup() {
		sortedStaticWeights = null;  
		this.sortedDynamicWeights = null;
		this.teasers = null;
	}
}
