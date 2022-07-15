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

package org.apache.dolphinscheduler.plugin.task.sparksql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.dolphinscheduler.plugin.task.util.ArgsUtils;
import org.apache.dolphinscheduler.spi.utils.DateUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

/**
 * spark args utils
 */
public class SparkSqlArgsUtils {

    private static final String SPARK_CLUSTER = "cluster";

    private static final String SPARK_LOCAL = "local";

    private static final String SPARK_ON_YARN = "yarn";
    
    
	final static String MAIN_CLASS = "com.tong.bigdata.spark.sql.SparkSqlApp";
	final static String MAIN_JAR = "bigdata-sparksql-1.0.0-jar-with-dependencies.jar";
	final static ProgramType PROGRAM_TYPE = ProgramType.JAVA;
	final static String SPARKCONF_TASKFILE = "--conf spark.taskfile";
	final static String SPARK_FILES = "--files";
	final static String MAIN_ARGS_ID = "--id";
	final static String MAIN_ARGS_TYPE = "--task_type json";
	

    private SparkSqlArgsUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * build args
     *
     * @param param param
     * @return argument list
     */
    public static List<String> buildArgs(String jsonFilePath, String tsparkHomePath, SparkSqlParameters param) {
        List<String> args = new ArrayList<>();
        args.add(SparkSqlConstants.MASTER);

        String deployMode = StringUtils.isNotEmpty(param.getDeployMode()) ? param.getDeployMode() : SPARK_CLUSTER;
        if (!SPARK_LOCAL.equals(deployMode)) {
            args.add(SPARK_ON_YARN);
            args.add(SparkSqlConstants.DEPLOY_MODE);
        }
        args.add(deployMode);

        args.add(SparkSqlConstants.MAIN_CLASS);
        args.add(MAIN_CLASS);

        int driverCores = param.getDriverCores();
        if (driverCores > 0) {
            args.add(SparkSqlConstants.DRIVER_CORES);
            args.add(String.format("%d", driverCores));
        }

        String driverMemory = param.getDriverMemory();
        if (StringUtils.isNotEmpty(driverMemory)) {
            args.add(SparkSqlConstants.DRIVER_MEMORY);
            args.add(driverMemory);
        }

        int numExecutors = param.getNumExecutors();
        if (numExecutors > 0) {
            args.add(SparkSqlConstants.NUM_EXECUTORS);
            args.add(String.format("%d", numExecutors));
        }

        int executorCores = param.getExecutorCores();
        if (executorCores > 0) {
            args.add(SparkSqlConstants.EXECUTOR_CORES);
            args.add(String.format("%d", executorCores));
        }

        String executorMemory = param.getExecutorMemory();
        if (StringUtils.isNotEmpty(executorMemory)) {
            args.add(SparkSqlConstants.EXECUTOR_MEMORY);
            args.add(executorMemory);
        }

        String appName = param.getAppName();
        if (StringUtils.isNotEmpty(appName)) {
            args.add(SparkSqlConstants.SPARK_NAME);
            args.add(ArgsUtils.escape(appName));
        }

        String others = param.getOthers();
        if (!SPARK_LOCAL.equals(deployMode) && (StringUtils.isEmpty(others) || !others.contains(SparkSqlConstants.SPARK_QUEUE))) {
            String queue = param.getQueue();
            if (StringUtils.isNotEmpty(queue)) {
                args.add(SparkSqlConstants.SPARK_QUEUE);
                args.add(queue);
            }
        }

        // --conf --files --jars --packages
        if (StringUtils.isNotEmpty(others)) {
            args.add(others);
        }
        // add default system args
        addDefaultExtendArgs(args, others, jsonFilePath);

        args.add(tsparkHomePath + MAIN_JAR);

        String mainArgs = param.getMainArgs();
        if (StringUtils.isNotEmpty(mainArgs)) {
            args.add(mainArgs);
        }

        if(StringUtils.isBlank(appName)) {
        	appName = "tongcs-sparkcs-" + DateUtils.format(new Date(), "yyyyMMddHHmmss");
        }
        args.add(String.format("%s %s", MAIN_ARGS_ID,appName));
        args.add(MAIN_ARGS_TYPE);

        return args;
    }

	private static void addDefaultExtendArgs(List<String> args, String others, String jsonFilePath) {
		//
		if(others!=null && !others.contains("spark.network.timeout")) {
			args.add("--conf spark.network.timeout=10000000");
		}
		if(others!=null && !others.contains("spark.executor.heartbeatInterval")) {
			args.add("--conf spark.executor.heartbeatInterval=10000000");
		}
		if(others!=null && !others.contains("spark.executor.memoryOverhead")) {
			args.add("--conf spark.executor.memoryOverhead=1536");
		}
		if(others!=null && !others.contains("spark.dynamicAllocation.executorIdleTimeout")) {
			args.add("--conf spark.dynamicAllocation.executorIdleTimeout=600");
		}
		if(others!=null && !others.contains("spark.sql.adaptive.enabled")) {
			args.add("--conf spark.sql.adaptive.enabled=true");
		}
		if(others!=null && !others.contains("spark.sql.adaptive.shuffle.targetPostShuffleInputSize")) {
			args.add("--conf spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728b");
		}
		if(others!=null && !others.contains("spark.sql.adaptive.join.enabled")) {
			args.add("--conf spark.sql.adaptive.join.enabled=true");
		}
		// add files
		args.add(String.format("%s=%s",SPARKCONF_TASKFILE, jsonFilePath));
		args.add(String.format("%s %s", SPARK_FILES, jsonFilePath));
	}

}
