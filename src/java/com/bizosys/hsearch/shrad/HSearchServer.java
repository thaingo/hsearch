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
package com.bizosys.hsearch.shrad;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

/**
 * @author karan
 *
 */
public class HSearchServer implements HttpHandler {

	public static Logger l = Logger.getLogger(HSearchServer.class.getName());
	
	public HSearchServer() {
	}
	
	public void handle(HttpExchange exchange) throws IOException {
		try {
			l.info("A=" + exchange.getRequestMethod());
			l.info("P=" + exchange.getHttpContext().getPath());
			l.info("E=" + exchange.getHttpContext().getAttributes().keySet().toString() );
			String action = (String) 
				exchange.getHttpContext().getAttributes().get("action");
			String command = (String) exchange.getAttribute("command");
			if ( l.isInfoEnabled()) {
				l.info("HSearch Server Action: " + action + " , Command: " + command);
			}
			
            // Set response headers
            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.set("Content-Type", "text/xml");
            exchange.sendResponseHeaders(200, 0);

            // Get response body
            OutputStream responseBody = exchange.getResponseBody();
			responseBody.write("<html><body>Working</body></html>".getBytes());
			responseBody.close();
		}catch (Exception e) {
			throw new IOException(e);
		} finally {
			exchange.close();			
		}
	}
	

	public static void main(String[] args) throws Exception {
		
		HSearchServer pxy = new HSearchServer();
		InetSocketAddress addr = new InetSocketAddress(7070);
		HttpServer server = HttpServer.create(addr, 0);

		server.createContext("/process.do", pxy );
		server.setExecutor(Executors.newCachedThreadPool());
		server.start();
		System.out.println("Server is listening on port 7070");
	}

}
