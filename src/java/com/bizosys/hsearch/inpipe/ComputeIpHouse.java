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

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.util.IpUtil;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Deduce the Host IP address based on URL. 
 * It converts the IP address to a single integer (IP House)
 * @see IpUtil	 
 * @author karan
 *
 */
public class ComputeIpHouse implements PipeIn {

	public boolean commit() { 
		return true; 	
	}	

	public PipeIn getInstance() { 
		return this; 	
	}

	public String getName() { 
		return "ComputeIpHouse"; 	
	}

	public boolean init(Configuration conf)  { 
		return true; 
	}

	public boolean visit(Object docObj) {
		if ( null == docObj) return false;
		Doc doc = (Doc) docObj;
		
    	DocMeta meta = doc.meta;
    	if ( null == meta ) return false;
    	
    	DocTeaser teaser = doc.teaser;
    	if ( null == teaser ) return false;

    	if ( null == teaser.url ) return true;
    	try {
	    	String url = teaser.url.getValue().toString();
	    	String urlL =url.toLowerCase();
	    	if ( ! ( urlL.startsWith("http") || 
	    		urlL.startsWith("ftp") ) )  return true; 
	    	URL resolvedUrl = new URL(url); 
	    	InetAddress ipaddress = InetAddress.getByName(resolvedUrl.getHost());
	    	meta.ipHouse = IpUtil.computeHouse(ipaddress.getHostAddress());
	    	return true;
    	} catch (UnknownHostException ex) {
    		InpipeLog.l.info(ex);
    		return false;
    	} catch (MalformedURLException ex) {
    		InpipeLog.l.info(ex);
    		return false;
    	}
	}
	
}
