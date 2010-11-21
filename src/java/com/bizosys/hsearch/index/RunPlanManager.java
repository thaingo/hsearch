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
package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.inpipe.L;

public class RunPlanManager implements Service {
	
	private static RunPlanManager instance = null;
	
	public static RunPlanManager getInstance() {
		if ( null != instance) return instance;
		synchronized (RunPlanManager.class) {
			if ( null != instance) return instance;
			instance = new RunPlanManager();
		}
		return instance;
	}
	
	Map<String, PipeIn> pipes = new HashMap<String, PipeIn>(); 
	
	public RunPlanManager(){
	}
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		String pipes = conf.get("inpipes", "");
		String[] pipeL = StringUtils.getStrings(pipes);
		if ( null == pipeL) return true;
		try {
			for (String pipe : pipeL) {
				L.l.info("Initializing Pipe :" + pipe);
				PipeIn inpipe = (PipeIn) Class.forName(pipe).newInstance();
				if ( ! inpipe.init(conf) ) {
					L.l.fatal("Pipe Initialization Failure :" + pipe);
					return false;
				}
				this.pipes.put(inpipe.getName(), inpipe);
			}
			return true;
		} catch (Exception e) {
			L.l.fatal("Pipe Initialization Failure :" , e );
			return false;
		}		
	}

	public void process(Request req, Response res) {
		
	}

	public void stop() {
		this.pipes.clear();
	}	
	
	/**
	 * Gives all the available steps for processing
	 * @return
	 */
	public Set<String> getAvailableSteps() {
		return pipes.keySet();
	}
	
	/**
	 * Resolves the plan for a given step names.
	 * TODO:// Sanity check for the intelligencec of step sequencing..
	 * @param stepNames
	 * @return
	 * @throws ApplicationFault
	 */
	public List<PipeIn> compilePlan(String[] stepNames) throws ApplicationFault {
		
		List<PipeIn> anvils = new ArrayList<PipeIn>(stepNames.length);
		for (String step : stepNames) {
			PipeIn aPipe = pipes.get(step);
			if ( null == aPipe) throw new ApplicationFault("Pipe Not Found: " + step);
			anvils.add(aPipe.getInstance());
		}
		return anvils;
	}
	
	public List<PipeIn> compilePlan(List<String> stepNames) throws ApplicationFault {
		
		List<PipeIn> anvils = new ArrayList<PipeIn>(stepNames.size());
		for (String step : stepNames) {
			PipeIn aPipe = pipes.get(step);
			if ( null == aPipe) throw new ApplicationFault("Pipe Not Found: " + step);
			anvils.add(aPipe);
		}
		return anvils;
	}

	public String getName() {
		return "RunPlanManager";
	}
	
}
