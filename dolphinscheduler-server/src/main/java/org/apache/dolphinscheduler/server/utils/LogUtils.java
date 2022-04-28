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

package org.apache.dolphinscheduler.server.utils;

import org.apache.dolphinscheduler.server.log.TaskLogDiscriminator;
import org.apache.dolphinscheduler.service.queue.entity.TaskExecutionContext;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.sift.SiftingAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.spi.AppenderAttachable;

public class LogUtils {
	private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);
    private LogUtils() throws IllegalStateException {
        throw new IllegalStateException("Utility class");
    }

    /**
     * get task log path
     */
    public static String getTaskLogPath(Long processDefineCode, int processDefineVersion, int processInstanceId, int taskInstanceId) {
    	/**
    	ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    	if(loggerFactory == null) {
    		logger.info("taskInstanceId: {} loggerFactory is null.", taskInstanceId);
    		return "";
    	}
    	AppenderAttachable<ILoggingEvent> tmp1 = (AppenderAttachable<ILoggingEvent>) (loggerFactory.getLogger("ROOT"));
    	SiftingAppender tmp2 = (SiftingAppender) tmp1.getAppender("TASKLOGFILE");
    	TaskLogDiscriminator tmp3 = (TaskLogDiscriminator) tmp2.getDiscriminator();
    	String logBase = tmp3.getLogBase();
    	Path returnPath = Paths.get(logBase).toAbsolutePath().resolve(processDefineCode + "_" + processDefineVersion)
    	.resolve(String.valueOf(processInstanceId))
        .resolve(taskInstanceId + ".log");
    	String returnLog = returnPath.toString();
    	logger.info("taskInstanceId: {}, returnLog:{} ", taskInstanceId, returnLog);
    	*/
        // Optional.map will be skipped if null
        return Optional.of(LoggerFactory.getILoggerFactory())
                .map(e -> (AppenderAttachable<ILoggingEvent>) (e.getLogger("ROOT")))
                .map(e -> (SiftingAppender) (e.getAppender("TASKLOGFILE")))
                .map(e -> ((TaskLogDiscriminator) (e.getDiscriminator())))
                .map(TaskLogDiscriminator::getLogBase)
                .map(e -> Paths.get(e)
                        .toAbsolutePath()
                        .resolve(processDefineCode + "_" + processDefineVersion)
                        .resolve(String.valueOf(processInstanceId))
                        .resolve(taskInstanceId + ".log"))
                .map(Path::toString)
                .orElse("");
    }

    /**
     * get task log path by TaskExecutionContext
     */
    public static String getTaskLogPath(TaskExecutionContext taskExecutionContext) {
        return getTaskLogPath(taskExecutionContext.getProcessDefineCode(),
                taskExecutionContext.getProcessDefineVersion(),
                taskExecutionContext.getProcessInstanceId(),
                taskExecutionContext.getTaskInstanceId());
    }

}
