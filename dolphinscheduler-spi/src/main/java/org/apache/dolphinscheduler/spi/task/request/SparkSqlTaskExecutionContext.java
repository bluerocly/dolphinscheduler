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

package org.apache.dolphinscheduler.spi.task.request;


import java.io.Serializable;
import java.util.List;

/**
 *  SparkSql Task ExecutionContext
 */
public class SparkSqlTaskExecutionContext implements Serializable {


    private List<String> udfNames;

    /**
     * DefaultFS
     */
    private String defaultFS;


    public List<String> getUdfNames() {
        return udfNames;
    }

    public void setUdfNames(List<String> udfNames) {
        this.udfNames = udfNames;
    }

    public String getDefaultFS() {
        return defaultFS;
    }

    public void setDefaultFS(String defaultFS) {
        this.defaultFS = defaultFS;
    }

    @Override
    public String toString() {
        return "SparkSqlTaskExecutionContext{"
                + ", udfNames =" + udfNames
                + ", defaultFS='" + defaultFS + '\'' + '}';
    }
}
