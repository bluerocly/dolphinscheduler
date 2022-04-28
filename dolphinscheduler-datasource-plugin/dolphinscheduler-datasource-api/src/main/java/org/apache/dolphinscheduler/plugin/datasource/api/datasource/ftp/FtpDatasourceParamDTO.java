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

import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseFtpDatasourceParamDTO;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.enums.FtpConnectType;

public class FtpDatasourceParamDTO extends BaseFtpDatasourceParamDTO {

    private FtpConnectType ftpConnectType;


    public FtpConnectType getFtpConnectType() {
		return ftpConnectType;
	}

	public void setFtpConnectType(FtpConnectType ftpConnectType) {
		this.ftpConnectType = ftpConnectType;
	}

	@Override
    public String toString() {
        return "FtpDatasourceParamDTO{"
                + "name='" + name + '\''
                + ", note='" + note + '\''
                + ", host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", userName='" + userName + '\''
                + ", password='" + password + '\''
                + ", ftpConnectType=" + ftpConnectType
                + ", other='" + other + '\''
                + '}';
    }

    @Override
    public DbType getType() {
        return DbType.FTP;
    }
}
