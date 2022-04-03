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

package org.apache.dolphinscheduler.plugin.datasource.api.datasource.greenplum;

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDatasourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.collections4.MapUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.HashMap;
public class GreenplumDatasourceProcessor extends AbstractDatasourceProcessor {

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        GreenplumConnectionParam connectionParams = (GreenplumConnectionParam) createConnectionParams(connectionJson);

        GreenplumDatasourceParamDTO greenplumDatasourceParamDTO = new GreenplumDatasourceParamDTO();
        greenplumDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        greenplumDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));
        greenplumDatasourceParamDTO.setUserName(greenplumDatasourceParamDTO.getUserName());

        String[] hostSeperator = connectionParams.getAddress().split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        greenplumDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);
        greenplumDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));

        return greenplumDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        GreenplumDatasourceParamDTO greenplumParam = (GreenplumDatasourceParamDTO) datasourceParam;
        String address = String.format("%s%s:%s", Constants.JDBC_GREENPLUM, greenplumParam.getHost(), greenplumParam.getPort());
        // Change to the greenplum driver's pattern.
//        String jdbcUrl = String.format("%s/%s", address, greenplumParam.getDatabase());
        String jdbcUrl = address;
        if(MapUtils.isEmpty(greenplumParam.getOther())) {
        	greenplumParam.setOther(new HashMap<String,String>());
        }
        greenplumParam.getOther().put("DatabaseName", greenplumParam.getDatabase());
        GreenplumConnectionParam greenplumConnectionParam = new GreenplumConnectionParam();
        greenplumConnectionParam.setAddress(address);
        greenplumConnectionParam.setDatabase(greenplumParam.getDatabase());
        greenplumConnectionParam.setJdbcUrl(jdbcUrl);
        greenplumConnectionParam.setUser(greenplumParam.getUserName());
        greenplumConnectionParam.setPassword(PasswordUtils.encodePassword(greenplumParam.getPassword()));
        greenplumConnectionParam.setDriverClassName(getDatasourceDriver());
        greenplumConnectionParam.setValidationQuery(getValidationQuery());
        greenplumConnectionParam.setOther(transformOther(greenplumParam.getOther()));
        greenplumConnectionParam.setProps(greenplumParam.getOther());

        return greenplumConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, GreenplumConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return Constants.COM_GREENPLUM_JDBC_DRIVER;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        GreenplumConnectionParam greenplumConnectionParam = (GreenplumConnectionParam) connectionParam;
        if (!StringUtils.isEmpty(greenplumConnectionParam.getOther())) {
            return String.format("%s;%s", greenplumConnectionParam.getJdbcUrl(), greenplumConnectionParam.getOther());
        }
        return greenplumConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        GreenplumConnectionParam greenplumConnectionParam = (GreenplumConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(greenplumConnectionParam),
        		greenplumConnectionParam.getUser(), PasswordUtils.decodePassword(greenplumConnectionParam.getPassword()));
    }

    @Override
    public DbType getDbType() {
        return DbType.GREENPLUM;
    }

    @Override
    public String getValidationQuery() {
        return Constants.GREENPLUM_VALIDATION_QUERY;
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s%s", key, value, ";")));
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    private Map<String, String> parseOther(String other) {
        if (other == null) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        for (String config : other.split("&")) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }
}
