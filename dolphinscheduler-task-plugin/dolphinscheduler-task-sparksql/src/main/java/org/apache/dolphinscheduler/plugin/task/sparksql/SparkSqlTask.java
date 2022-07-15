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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractYarnTask;
import org.apache.dolphinscheduler.plugin.task.util.MapUtils;
import org.apache.dolphinscheduler.spi.enums.Flag;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.paramparser.ParamUtils;
import org.apache.dolphinscheduler.spi.task.paramparser.ParameterUtils;
import org.apache.dolphinscheduler.spi.task.request.SparkSqlTaskExecutionContext;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

public class SparkSqlTask extends AbstractYarnTask {

    /**
     * spark2 command
     * usage: spark-submit [options] <app jar | python file> [app arguments]
     */
    private static final String SPARK2_COMMAND = "${SPARK_HOME2}/bin/spark-submit";
    private static final String TSPARKSQL_HOME = "${TSPARKSQL_HOME}/";

    /**
     * spark parameters
     */
    private SparkSqlParameters sparkSqlParameters;

    /**
     * taskExecutionContext
     */
    private TaskRequest taskExecutionContext;

    public SparkSqlTask(TaskRequest taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        logger.info("spark task params {}", taskExecutionContext.getTaskParams());
        sparkSqlParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), SparkSqlParameters.class);

        if (null == sparkSqlParameters) {
            logger.error("Spark params is null");
            return;
        }

        if (!sparkSqlParameters.checkParameters()) {
            throw new RuntimeException("spark task params is not valid");
        }
        sparkSqlParameters.setQueue(taskExecutionContext.getQueue());
        setMainJarName();
    }

    /**
     * create command
     * @return command
     */
    @Override
    protected String buildCommand() {
        // spark-submit [options] <app jar | python file> [app arguments]
        List<String> args = new ArrayList<>();

        // spark version
        String sparkCommand = SPARK2_COMMAND;
        args.add(sparkCommand);

     // replace placeholder, and combining local and global parameters
        Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext,getParameters());
        if (MapUtils.isEmpty(paramsMap)) {
            paramsMap = new HashMap<>();
        }
        if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
            paramsMap.putAll(taskExecutionContext.getParamsMap());
        }
        
        try {
			String jsonFilePath = buildSparkSqlJsonFile(paramsMap);
			// other parameters
			args.addAll(SparkSqlArgsUtils.buildArgs(jsonFilePath, TSPARKSQL_HOME, sparkSqlParameters));
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        String command = ParameterUtils.convertParameterPlaceholders(String.join(" ", args), ParamUtils.convert(paramsMap));
        logger.info("spark task command: {}", command);

        return command;
    }

    private String buildSparkSqlJsonFile(Map<String, Property> paramsMap)
            throws Exception {
        // generate json
        String fileName = String.format("%s/%s_task.json",
                taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId());
        String json;

        Path path = new File(fileName).toPath();
        if (Files.exists(path)) {
            return fileName;
        }

        SparkSqlTaskExecutionContext sparkSqlTaskExecutionContext = taskExecutionContext.getSparkSqlTaskExecutionContext();
        
        if (sparkSqlParameters.getCustomConfig() == Flag.YES.ordinal()) {
            json = sparkSqlParameters.getJson().replaceAll("\\r\\n", "\n");
        } else {
        	HashMap<String,Object> map =  new HashMap<>();
        	map.put("taskId", sparkSqlParameters.getAppName());
//        	map.put("taskDesc", sparkSqlParameters.get);
        	map.put("mainSql", sparkSqlParameters.getSql());
        	if (CollectionUtils.isNotEmpty(sparkSqlParameters.getPreStatements())) {
        		map.put("preSqls", sparkSqlParameters.getPreStatements());
        	}
        	if (CollectionUtils.isNotEmpty(sparkSqlParameters.getPostStatements())) {
        		map.put("postSqls", sparkSqlParameters.getPostStatements());
        	}
        	if (!StringUtils.isEmpty(sparkSqlParameters.getUdfs())) {
        		List<String> udfNames = sparkSqlTaskExecutionContext.getUdfNames();
        		if (CollectionUtils.isNotEmpty(udfNames)) {
        			map.put("udfNames", sparkSqlParameters.getUdfs());
        		} else {
        			logger.warn("udfIds[{}], but udfNames is null.");
        		}
        	}
        	String jsonString = JSONUtils.toJsonString(map);
            json = jsonString;
        }

        // replace placeholder
        json = ParameterUtils.convertParameterPlaceholders(json, ParamUtils.convert(paramsMap));

        logger.debug("sparksql task json : {}", json);

        // create sparksql task json file
        FileUtils.writeStringToFile(new File(fileName), json, StandardCharsets.UTF_8);
        return fileName;
    }

	@Override
    protected void setMainJarName() {
//        // main jar
//        ResourceInfo mainJar = sparkParameters.getMainJar();
//
//        if (null == mainJar) {
//            throw new RuntimeException("Spark task jar params is null");
//        }
//
//        int resourceId = mainJar.getId();
//        String resourceName;
//        if (resourceId == 0) {
//            resourceName = mainJar.getRes();
//        } else {
//            //when update resource maybe has error
//            resourceName = mainJar.getResourceName().replaceFirst("/", "");
//        }
//        mainJar.setRes(resourceName);
//        sparkParameters.setMainJar(mainJar);

    }

    @Override
    public AbstractParameters getParameters() {
        return sparkSqlParameters;
    }
}
