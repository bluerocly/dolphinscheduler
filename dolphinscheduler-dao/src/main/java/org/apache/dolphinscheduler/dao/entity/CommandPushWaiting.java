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

package org.apache.dolphinscheduler.dao.entity;

import java.util.Date;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * command push waiting
 */
@TableName("t_ds_command_push_waiting")
public class CommandPushWaiting {
	
    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private int id;

    /**
     * process definition code
     */
    @TableField("process_definition_code")
    private long processDefinitionCode;

    /**
     * executor push id
     */
    @TableField("command_push_id")
    private int commandPushId;

    /**
     * dep_data_name
     */
    @TableField("dep_data_name")
    private String depDataName;

    /**
     * dep_data_count
     */
    @TableField("dep_data_count")
    private int depDataCount;

    
    /**
     * dep_data_period
     */
    @TableField("dep_data_period")
    private int depDataPeriod;
    
    /**
     * dep_data_time
     */
    @TableField("dep_data_time")
    private Date depDataTime;
    
    @TableField("dep_data_param")
    private int depDataParam;

    
    @TableField("insert_time")
    private Date insertTime;

    @TableField("update_time")
    private Date updateTime;

    /**
     * receive flag
     */
    @TableField("receive_flag")
    private int receiveFlag;
    
    @TableField("handled_flag")
    private int handledFlag;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getProcessDefinitionCode() {
		return processDefinitionCode;
	}

	public void setProcessDefinitionCode(long processDefinitionCode) {
		this.processDefinitionCode = processDefinitionCode;
	}

	public int getCommandPushId() {
		return commandPushId;
	}

	public void setCommandPushId(int commandPushId) {
		this.commandPushId = commandPushId;
	}

	public String getDepDataName() {
		return depDataName;
	}

	public void setDepDataName(String depDataName) {
		this.depDataName = depDataName;
	}

	public int getDepDataCount() {
		return depDataCount;
	}

	public void setDepDataCount(int depDataCount) {
		this.depDataCount = depDataCount;
	}

	public int getDepDataPeriod() {
		return depDataPeriod;
	}

	public void setDepDataPeriod(int depDataPeriod) {
		this.depDataPeriod = depDataPeriod;
	}

	public Date getDepDataTime() {
		return depDataTime;
	}

	public void setDepDataTime(Date depDataTime) {
		this.depDataTime = depDataTime;
	}

	public int getDepDataParam() {
		return depDataParam;
	}

	public void setDepDataParam(int depDataParam) {
		this.depDataParam = depDataParam;
	}

	public Date getInsertTime() {
		return insertTime;
	}

	public void setInsertTime(Date insertTime) {
		this.insertTime = insertTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public int getReceiveFlag() {
		return receiveFlag;
	}

	public void setReceiveFlag(int receiveFlag) {
		this.receiveFlag = receiveFlag;
	}

	public int getHandledFlag() {
		return handledFlag;
	}

	public void setHandledFlag(int handledFlag) {
		this.handledFlag = handledFlag;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + commandPushId;
		result = prime * result + depDataCount;
		result = prime * result + ((depDataName == null) ? 0 : depDataName.hashCode());
		result = prime * result + depDataParam;
		result = prime * result + depDataPeriod;
		result = prime * result + ((depDataTime == null) ? 0 : depDataTime.hashCode());
		result = prime * result + handledFlag;
		result = prime * result + id;
		result = prime * result + ((insertTime == null) ? 0 : insertTime.hashCode());
		result = prime * result + (int) (processDefinitionCode ^ (processDefinitionCode >>> 32));
		result = prime * result + receiveFlag;
		result = prime * result + ((updateTime == null) ? 0 : updateTime.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CommandPushWaiting other = (CommandPushWaiting) obj;
		if (commandPushId != other.commandPushId)
			return false;
		if (depDataCount != other.depDataCount)
			return false;
		if (depDataName == null) {
			if (other.depDataName != null)
				return false;
		} else if (!depDataName.equals(other.depDataName))
			return false;
		if (depDataParam != other.depDataParam)
			return false;
		if (depDataPeriod != other.depDataPeriod)
			return false;
		if (depDataTime == null) {
			if (other.depDataTime != null)
				return false;
		} else if (!depDataTime.equals(other.depDataTime))
			return false;
		if (handledFlag != other.handledFlag)
			return false;
		if (id != other.id)
			return false;
		if (insertTime == null) {
			if (other.insertTime != null)
				return false;
		} else if (!insertTime.equals(other.insertTime))
			return false;
		if (processDefinitionCode != other.processDefinitionCode)
			return false;
		if (receiveFlag != other.receiveFlag)
			return false;
		if (updateTime == null) {
			if (other.updateTime != null)
				return false;
		} else if (!updateTime.equals(other.updateTime))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CommandPushWaiting [id=" + id + ", processDefinitionCode=" + processDefinitionCode + ", commandPushId="
				+ commandPushId + ", depDataName=" + depDataName + ", depDataCount=" + depDataCount + ", depDataPeriod="
				+ depDataPeriod + ", depDataTime=" + depDataTime + ", depDataParam=" + depDataParam + ", insertTime="
				+ insertTime + ", updateTime=" + updateTime + ", receiveFlag=" + receiveFlag + ", handledFlag="
				+ handledFlag + "]";
	}
}

