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
package com.bizosys.hsearch.schema;

public class IOConstants {

	public static final String TABLE_DICTIONARY = "dictionary";
	public static final String TABLE_IDMAP = "idmap";
	public static final String TABLE_PREVIEW = "preview";
	public static final String TABLE_CONTENT = "content";
	public static final String TABLE_CONFIG = "config";

	/**
	 * Read this IO Section During Search
	 */
	public static final String SEARCH = "S";
	public static final byte[] SEARCH_BYTES = SEARCH.getBytes();

	public static final String ACL = "a";
	public static final byte[] ACL_BYTES = ACL.getBytes();

	public static final String META = "m";
	public static final byte[] META_BYTES = META.getBytes();
	
	/**
	 * Read this Section During Search Page Display
	 */
	public static final String TEASER = "T";
	public static final byte[] TEASER_BYTES = TEASER.getBytes();

	public static final char TEASER_ID = 'i';
	public static final byte[] TEASER_ID_BYTES = "i".getBytes();
	
	public static final char TEASER_URL = 'u';
	public static final byte[] TEASER_URL_BYTES = "u".getBytes();
	
	public static final char TEASER_TITLE = 't';
	public static final byte[] TEASER_TITLE_BYTES = "t".getBytes();

	public static final char TEASER_CACHE = 'c';
	public static final byte[] TEASER_CACHE_BYTES = "c".getBytes();
	
	public static final char TEASER_PREVIEW = 'p';
	public static final byte[] TEASER_PREVIEW_BYTES = "p".getBytes();

	/**
	 * Read this Section During Original Data
	 */
	
	public static final char CONTENT_FIELDS = 'd';
	public static final byte[] CONTENT_FIELDS_BYTES = "d".getBytes();

	public static final char CONTENT_CITATION = 'C';
	public static final byte[] CONTENT_CITATION_BYTES = "C".getBytes();
	
	public static final char CONTENT_CITATION_TO = 't';
	public static final byte[] CONTENT_CITATION_TO_BYTES = "t".getBytes();

	public static final char CONTENT_CITATION_FROM = 'f';
	public static final byte[] CONTENT_CITATION_FROM_BYTES = "f".getBytes();

	/**
	 * Dictionary
	 */
	public static final String DICTIONARY = "d";
	public static final byte[] DICTIONARY_BYTES = DICTIONARY.getBytes();

	public static final String DICTIONARY_TERM = "t";
	public static final byte[] DICTIONARY_TERM_BYTES = DICTIONARY_TERM.getBytes();

	public static final byte[] ALL_TERM_BYTES = "__ALL_EN".getBytes();

	/**
	 * Config Bytes
	 */
	public static final String NAME_VALUE = "n";
	public static final byte[] NAME_VALUE_BYTES = NAME_VALUE.getBytes();
}

