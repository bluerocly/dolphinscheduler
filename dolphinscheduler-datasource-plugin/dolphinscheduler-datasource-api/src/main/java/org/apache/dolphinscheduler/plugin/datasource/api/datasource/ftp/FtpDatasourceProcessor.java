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

package org.apache.dolphinscheduler.plugin.datasource.api.datasource.ftp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDatasourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.enums.FtpConnectType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

public class FtpDatasourceProcessor extends AbstractDatasourceProcessor {

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        FtpConnectionParam connectionParams = (FtpConnectionParam) createConnectionParams(connectionJson);
        FtpDatasourceParamDTO ftpDatasourceParamDTO = new FtpDatasourceParamDTO();

        ftpDatasourceParamDTO.setProtocol(connectionParams.getProtocol());
        ftpDatasourceParamDTO.setUserName(connectionParams.getUser());
        ftpDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));
        String hostSeperator = Constants.AT_SIGN;
        String[] hostPort = connectionParams.getAddress().split(hostSeperator);
        String[] hostPortArray = hostPort[hostPort.length - 1].split(Constants.COLON);
        ftpDatasourceParamDTO.setHost(hostPortArray[0]);
        ftpDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[1]));
        return ftpDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
    	FtpDatasourceParamDTO ftpParam = (FtpDatasourceParamDTO) datasourceParam;
    	String address;
    	if (FtpConnectType.FTP_NAME.equals(ftpParam.getFtpConnectType())) {
            address = String.format("%s%s:%s@%s:%s",
                    Constants.FTP, ftpParam.getUserName(), ftpParam.getPassword(),ftpParam.getHost(),ftpParam.getPort());
        } else {
            address = String.format("%s%s:%s@%s:%s",
                    Constants.SFTP, ftpParam.getUserName(), ftpParam.getPassword(),ftpParam.getHost(),ftpParam.getPort());
        }
    	
        FtpConnectionParam ftpConnectionParam = new FtpConnectionParam();
        ftpConnectionParam.setUser(ftpParam.getUserName());
        ftpConnectionParam.setPassword(PasswordUtils.encodePassword(ftpParam.getPassword()));
        ftpConnectionParam.setAddress(address);
        ftpConnectionParam.setProtocol(ftpParam.getProtocol());
        ftpConnectionParam.setFtpConnectType(ftpParam.getFtpConnectType());
        ftpConnectionParam.setOther(transformOther(ftpParam.getOther()));
        ftpConnectionParam.setProps(ftpParam.getOther());
        return ftpConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, FtpConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return "null";
    }

    @Override
    public String getValidationQuery() {
        return "null";
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        return "null";
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
//    	return DriverManager.getConnection(getJdbcUrl(connectionParam),
//    			oracleConnectionParam.getUser(), PasswordUtils.decodePassword(oracleConnectionParam.getPassword()));
        return null;
    }

    @Override
    public DbType getDbType() {
        return DbType.FTP;
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        List<String> list = new ArrayList<>();
        otherMap.forEach((key, value) -> list.add(String.format("%s=%s", key, value)));
        return String.join("&", list);
    }

    private Map<String, String> parseOther(String other) {
        if (StringUtils.isEmpty(other)) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        String[] configs = other.split("&");
        for (String config : configs) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }
}
