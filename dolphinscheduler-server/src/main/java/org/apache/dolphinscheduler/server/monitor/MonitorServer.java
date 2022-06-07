/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.server.monitor;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 *  monitor server
 */
@ComponentScan(value = "org.apache.dolphinscheduler", excludeFilters = {
	    @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
	        "org.apache.dolphinscheduler.server.worker.*",
	        "org.apache.dolphinscheduler.server.master.*",
	        "org.apache.dolphinscheduler.server.log.*",
	        "org.apache.dolphinscheduler.alert.*"
	    })
	})
public class MonitorServer implements CommandLineRunner {

    private static final Integer ARGS_LENGTH = 4;

    private static final Logger logger = LoggerFactory.getLogger(MonitorServer.class);

    /**
     * monitor
     */
    @Autowired
    private Monitor monitor;



    public static void main(String[] args) throws Exception{

        new SpringApplicationBuilder(MonitorServer.class).web(WebApplicationType.NONE).run(args);
    }

    @Override
    public void run(String... args) throws Exception {
    	logger.info("args.length[{}] , ARGS_LENGTH[{}]", args.length, ARGS_LENGTH);
        if (args.length < ARGS_LENGTH){
            logger.error("Usage: <masterPath> <workerPath> <port> <installPath> <sleepSecond>");
            return;
        }

        String masterPath = args[0];
        String workerPath = args[1];
        Integer port = Integer.parseInt(args[2]);
        String installPath = args[3];
        int sleepSecond = 0;
        if(args.length == ARGS_LENGTH + 1) {
        	sleepSecond = Integer.parseInt(args[4]);
        }
        monitor.monitor(masterPath,workerPath,port,installPath);
        logger.info("begin check with {} seconds." , sleepSecond);
        while(sleepSecond > 0) {
        	logger.info("begin check...");
        	TimeUnit.SECONDS.sleep(sleepSecond);
        	monitor.monitor(masterPath,workerPath,port,installPath);
        	logger.info("end check.");
        }
        System.exit(0);
    }
}
