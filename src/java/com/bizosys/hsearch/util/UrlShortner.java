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
package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.bizosys.oneline.util.StringUtils;

/**
 *	Url shortner reads urlmappings file and applies shortening <br />
 *  Example (The URL and tab separated short code) <br />
 *  http://www.bizosys.com/employee.xml/id=	01 <br />
 *  http://www.bizosys.com/employee?	02 <br />
 *  http://www.bizosys.com/company.xml/id=	03 <br />
 *  
 * @author Abinasha Karana
 */
public class UrlShortner {

	public static Logger l = Logger.getLogger(UrlShortner.class.getName());
	private static UrlShortner instance = null;
	
	public static final UrlShortner getInstance()  {
		if ( null != instance) return instance;
		synchronized (UrlShortner.class) {
			if ( null != instance ) return instance;
			instance = new UrlShortner();
			instance.load();
		}
		return instance;
	}
	
	public HashMap<String, String> urls = null;
	public HashMap<String, String> codes = null;
	private final static List<String> EMPTY_ARRAY =  new ArrayList<String>();
	
	public void load() {
		List<String> mappings = null;
		try {
			mappings = FileReaderUtil.toLines("urlmappings");
		} catch (Exception ex) {
			l.warn("UrlMapper: Could not read urlmappings file.", ex);
			mappings =  EMPTY_ARRAY;
		}
		urls = new HashMap<String, String>(mappings.size());
		codes = new HashMap<String, String>(mappings.size());
		for (String aMap : mappings) {
			String[] mapVal = StringUtils.getStrings(aMap, '\t');
			urls.put(mapVal[0] , mapVal[1]);
			codes.put(mapVal[1], mapVal[0]);
		}
	}
	
	/**
	 * This encodes to the short form of the URL prefix
	 * @param url	URL
	 * @return	Encoded Url
	 */
	public String encoding(String url) {
		
		if ( StringUtils.isEmpty(url)) return null;
		
		
		//Can I get an exact match
		
		if ( urls != null && urls.containsKey(url) ) return urls.get(url);
		
		//Can I get an exact till the last = character.
		int lastEqualto = url.lastIndexOf('=');
		if ( -1 != lastEqualto) {
			lastEqualto = lastEqualto + 1;
			String prefix = url.substring(0,lastEqualto);
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' + url.substring(lastEqualto) ;
		}
		
		//Can I get an exact till the last / character.
		int lastSlash = url.lastIndexOf('?');
		if ( -1 != lastSlash) {
			lastSlash++;
			String prefix = url.substring(0,lastSlash );
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' +url.substring(lastSlash) ;
		}
		
		return url;
	}
	
	/**
	 * This decodes the short form of the URL prefix
	 * @param codedUrl	coded URL
	 * @return	The decoded url
	 */
	public String decoding(String codedUrl) {
		if ( StringUtils.isEmpty(codedUrl)) return null;
		
		//Is thre a direct match
		if ( codes.containsKey(codedUrl) ) return codes.get(codedUrl);
		int division = codedUrl.lastIndexOf('~');
		if ( -1 == division) return codedUrl;
		String code = codedUrl.substring(0,division );
		
		if ( codes.containsKey(code) ) 
			return codes.get(code) + codedUrl.substring(division + 1) ;
		
		return codedUrl;
	}
}
