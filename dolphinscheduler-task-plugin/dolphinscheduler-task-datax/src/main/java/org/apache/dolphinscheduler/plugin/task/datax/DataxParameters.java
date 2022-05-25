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

package org.apache.dolphinscheduler.plugin.task.datax;

import org.apache.dolphinscheduler.spi.enums.Flag;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.ResourceInfo;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * DataX parameter
 */
public class DataxParameters extends AbstractParameters {

    /**
     * if custom json config，eg  0, 1
     */
    private int customConfig;

    /**
     * if customConfig eq 1 ,then json is usable
     */
    private String json;

    /**
     * data source type，eg  MYSQL, POSTGRES ...
     */
    private String dsType;

    /**
     * datasource id
     */
    private int dataSource;

    /**
     * data target type，eg  MYSQL, POSTGRES ...增加FTP
     */
    private String dtType;

    /**
     * datatarget id
     */
    private int dataTarget;
    
    /**
     * sql
     */
    private String sql;

    /**
     * target table
     */
    private String targetTable;

    /**
     * Pre Statements
     */
    private List<String> preStatements;

    /**
     * Post Statements
     */
    private List<String> postStatements;

    /**
     * speed byte num
     */
    private int jobSpeedByte;

    /**
     * speed record count
     */
    private int jobSpeedRecord;

    /**
     * Xms memory
     */
    private int xms;

    /**
     * Xmx memory
     */
    private int xmx;
    
    private String fileName;
    private String fieldDelimiter;
    private String ftpEncoding;
    private String ftpHeader;
    private String ftpFileSuffix;
    private String subdirectory;
    private Boolean notification;
    private String queueName;
    private int groupId;
    private String messagejson;
    
    
    @Override
    public void dealOutParam(String result) {
    	// TODO 使用父类，后续根据需求进行处理
    	super.dealOutParam(result);
    }
    
	public String getFtpFileSuffix() {
		return ftpFileSuffix;
	}



	public void setFtpFileSuffix(String ftpFileSuffix) {
		this.ftpFileSuffix = ftpFileSuffix;
	}



	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	public String getFtpEncoding() {
		return ftpEncoding;
	}

	public void setFtpEncoding(String ftpEncoding) {
		this.ftpEncoding = ftpEncoding;
	}

	public String getFtpHeader() {
		return ftpHeader;
	}

	public void setFtpHeader(String ftpHeader) {
		this.ftpHeader = ftpHeader;
	}

	public String getSubdirectory() {
		return subdirectory;
	}

	public void setSubdirectory(String subdirectory) {
		this.subdirectory = subdirectory;
	}

	public Boolean getNotification() {
		return notification;
	}

	public void setNotification(Boolean notification) {
		this.notification = notification;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}

	public String getMessagejson() {
		return messagejson;
	}

	public void setMessagejson(String messagejson) {
		this.messagejson = messagejson;
	}

	public int getCustomConfig() {
        return customConfig;
    }

    public void setCustomConfig(int customConfig) {
        this.customConfig = customConfig;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getDsType() {
        return dsType;
    }

    public void setDsType(String dsType) {
        this.dsType = dsType;
    }

    public int getDataSource() {
        return dataSource;
    }

    public void setDataSource(int dataSource) {
        this.dataSource = dataSource;
    }

    public String getDtType() {
        return dtType;
    }

    public void setDtType(String dtType) {
        this.dtType = dtType;
    }

    public int getDataTarget() {
        return dataTarget;
    }

    public void setDataTarget(int dataTarget) {
        this.dataTarget = dataTarget;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public List<String> getPreStatements() {
        return preStatements;
    }

    public void setPreStatements(List<String> preStatements) {
        this.preStatements = preStatements;
    }

    public List<String> getPostStatements() {
        return postStatements;
    }

    public void setPostStatements(List<String> postStatements) {
        this.postStatements = postStatements;
    }

    public int getJobSpeedByte() {
        return jobSpeedByte;
    }

    public void setJobSpeedByte(int jobSpeedByte) {
        this.jobSpeedByte = jobSpeedByte;
    }

    public int getJobSpeedRecord() {
        return jobSpeedRecord;
    }

    public void setJobSpeedRecord(int jobSpeedRecord) {
        this.jobSpeedRecord = jobSpeedRecord;
    }

    public int getXms() {
        return xms;
    }

    public void setXms(int xms) {
        this.xms = xms;
    }

    public int getXmx() {
        return xmx;
    }

    public void setXmx(int xmx) {
        this.xmx = xmx;
    }

    @Override
    public boolean checkParameters() {
        if (customConfig == Flag.NO.ordinal()) {
            return dataSource != 0
                    && dataTarget != 0
                    && StringUtils.isNotEmpty(sql)
                    && !(StringUtils.isEmpty(targetTable) && StringUtils.isEmpty(subdirectory));
        } else {
            return StringUtils.isNotEmpty(json);
        }
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return new ArrayList<>();
    }

    @Override
    public String toString() {
        return "DataxParameters{"
                + "customConfig=" + customConfig
                + ", json='" + json + '\''
                + ", dsType='" + dsType + '\''
                + ", dataSource=" + dataSource
                + ", dtType='" + dtType + '\''
                + ", dataTarget=" + dataTarget
                + ", sql='" + sql + '\''
                + ", targetTable='" + targetTable + '\''
                + ", preStatements=" + preStatements
                + ", postStatements=" + postStatements
                + ", jobSpeedByte=" + jobSpeedByte
                + ", jobSpeedRecord=" + jobSpeedRecord
                + ", xms=" + xms
                + ", xmx=" + xmx
                + '}';
    }
}
