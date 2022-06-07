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

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * command push
 */
@TableName("t_ds_command_push")
public class CommandPush {
	
	
	
	public CommandPush() {
       
	}

    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    public int id;

    /**
     * command type
     */
    @TableField("command_type")
    public CommandType commandType;

    /**
     * process definition code
     */
    @TableField("process_definition_code")
    public long processDefinitionCode;

    /**
     * executor id
     */
    @TableField("executor_id")
    public int executorId;

    /**
     * command parameter, format json
     */
    @TableField("command_param")
    public String commandParam;

    /**
     * task depend type
     */
    @TableField("task_depend_type")
    public TaskDependType taskDependType;

    /**
     * failure strategy
     */
    @TableField("failure_strategy")
    public FailureStrategy failureStrategy;

    /**
     * warning type
     */
    @TableField("warning_type")
    public WarningType warningType;

    /**
     * warning group id
     */
    @TableField("warning_group_id")
    public Integer warningGroupId;

    /**
     * schedule time
     */
    @TableField("schedule_time")
    public Date scheduleTime;

    /**
     * start time
     */
    @TableField("start_time")
    public Date startTime;

    /**
     * process instance priority
     */
    @TableField("process_instance_priority")
    public Priority processInstancePriority;

    /**
     * update time
     */
    @TableField("update_time")
    public Date updateTime;

    /**
     * worker group
     */
    @TableField("worker_group")
    public String workerGroup;

    /**
     * environment code
     */
    @TableField("environment_code")
    public Long environmentCode;

    /**
     * dry run flag
     */
    @TableField("dry_run")
    public int dryRun;

    @TableField("process_instance_id")
    public int processInstanceId;

    @TableField("process_definition_version")
    public int processDefinitionVersion;

    /**
     * dep
     */
    @TableField("dep_data_names")
    private String depDataNames;

    @TableField("online_flag")
    private int onlineFlag;
    
    @TableField("dep_data_time_replaced_name")
    private String depDataTimeReplacedName;
    

    public TaskDependType getTaskDependType() {
        return taskDependType;
    }

    public void setTaskDependType(TaskDependType taskDependType) {
        this.taskDependType = taskDependType;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public long getProcessDefinitionCode() {
        return processDefinitionCode;
    }

    public void setProcessDefinitionCode(long processDefinitionCode) {
        this.processDefinitionCode = processDefinitionCode;
    }

    public FailureStrategy getFailureStrategy() {
        return failureStrategy;
    }

    public void setFailureStrategy(FailureStrategy failureStrategy) {
        this.failureStrategy = failureStrategy;
    }

    public void setCommandParam(String commandParam) {
        this.commandParam = commandParam;
    }

    public String getCommandParam() {
        return commandParam;
    }

    public WarningType getWarningType() {
        return warningType;
    }

    public void setWarningType(WarningType warningType) {
        this.warningType = warningType;
    }

    public Integer getWarningGroupId() {
        return warningGroupId;
    }

    public void setWarningGroupId(Integer warningGroupId) {
        this.warningGroupId = warningGroupId;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public int getExecutorId() {
        return executorId;
    }

    public void setExecutorId(int executorId) {
        this.executorId = executorId;
    }

    public Priority getProcessInstancePriority() {
        return processInstancePriority;
    }

    public void setProcessInstancePriority(Priority processInstancePriority) {
        this.processInstancePriority = processInstancePriority;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public Long getEnvironmentCode() {
        return this.environmentCode;
    }

    public void setEnvironmentCode(Long environmentCode) {
        this.environmentCode = environmentCode;
    }

    public int getDryRun() {
        return dryRun;
    }

    public void setDryRun(int dryRun) {
        this.dryRun = dryRun;
    }

    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public int getProcessDefinitionVersion() {
        return processDefinitionVersion;
    }

    public void setProcessDefinitionVersion(int processDefinitionVersion) {
        this.processDefinitionVersion = processDefinitionVersion;
    }
    
    public String getDepDataNames() {
		return depDataNames;
	}

	public void setDepDataNames(String depDataNames) {
		this.depDataNames = depDataNames;
	}

	public int getOnlineFlag() {
		return onlineFlag;
	}

	public void setOnlineFlag(int onlineFlag) {
		this.onlineFlag = onlineFlag;
	}

	public String getDepDataTimeReplacedName() {
		return depDataTimeReplacedName;
	}

	public void setDepDataTimeReplacedName(String depDataTimeReplacedName) {
		this.depDataTimeReplacedName = depDataTimeReplacedName;
	}

	@Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CommandPush command = (CommandPush) o;

        if (id != command.id) {
            return false;
        }
        if (processDefinitionCode != command.processDefinitionCode) {
            return false;
        }
        if (executorId != command.executorId) {
            return false;
        }
        if (workerGroup != null ? workerGroup.equals(command.workerGroup) : command.workerGroup == null) {
            return false;
        }

        if (environmentCode != null ? environmentCode.equals(command.environmentCode) : command.environmentCode == null) {
            return false;
        }

        if (commandType != command.commandType) {
            return false;
        }
        if (commandParam != null ? !commandParam.equals(command.commandParam) : command.commandParam != null) {
            return false;
        }
        if (taskDependType != command.taskDependType) {
            return false;
        }
        if (failureStrategy != command.failureStrategy) {
            return false;
        }
        if (warningType != command.warningType) {
            return false;
        }
        if (warningGroupId != null ? !warningGroupId.equals(command.warningGroupId) : command.warningGroupId != null) {
            return false;
        }
        if (scheduleTime != null ? !scheduleTime.equals(command.scheduleTime) : command.scheduleTime != null) {
            return false;
        }
        if (startTime != null ? !startTime.equals(command.startTime) : command.startTime != null) {
            return false;
        }
        if (processInstancePriority != command.processInstancePriority) {
            return false;
        }
        if (processInstanceId != command.processInstanceId) {
            return false;
        }
        if (processDefinitionVersion != command.getProcessDefinitionVersion()) {
            return false;
        }
        
        if (depDataNames != command.getDepDataNames()) {
            return false;
        }
        if (depDataTimeReplacedName != command.getDepDataTimeReplacedName()) {
            return false;
        }
        if (onlineFlag != command.getOnlineFlag()) {
            return false;
        }
        return !(updateTime != null ? !updateTime.equals(command.updateTime) : command.updateTime != null);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (commandType != null ? commandType.hashCode() : 0);
        result = 31 * result + Long.hashCode(processDefinitionCode);
        result = 31 * result + executorId;
        result = 31 * result + (commandParam != null ? commandParam.hashCode() : 0);
        result = 31 * result + (taskDependType != null ? taskDependType.hashCode() : 0);
        result = 31 * result + (failureStrategy != null ? failureStrategy.hashCode() : 0);
        result = 31 * result + (warningType != null ? warningType.hashCode() : 0);
        result = 31 * result + (warningGroupId != null ? warningGroupId.hashCode() : 0);
        result = 31 * result + (scheduleTime != null ? scheduleTime.hashCode() : 0);
        result = 31 * result + (startTime != null ? startTime.hashCode() : 0);
        result = 31 * result + (processInstancePriority != null ? processInstancePriority.hashCode() : 0);
        result = 31 * result + (updateTime != null ? updateTime.hashCode() : 0);
        result = 31 * result + (workerGroup != null ? workerGroup.hashCode() : 0);
        result = 31 * result + (environmentCode != null ? environmentCode.hashCode() : 0);
        result = 31 * result + dryRun;
        result = 31 * result + processInstanceId;
        result = 31 * result + processDefinitionVersion;
        result = 31 * result + (depDataNames != null ? depDataNames.hashCode() : 0);
        result = 31 * result + (depDataTimeReplacedName != null ? depDataTimeReplacedName.hashCode() : 0);
        result = 31 * result + onlineFlag;
        return result;
    }

    @Override
    public String toString() {
        return "CommandPush{"
                + "id=" + id
                + ", commandType=" + commandType
                + ", processDefinitionCode=" + processDefinitionCode
                + ", executorId=" + executorId
                + ", commandParam='" + commandParam + '\''
                + ", taskDependType=" + taskDependType
                + ", failureStrategy=" + failureStrategy
                + ", warningType=" + warningType
                + ", warningGroupId=" + warningGroupId
                + ", scheduleTime=" + scheduleTime
                + ", startTime=" + startTime
                + ", processInstancePriority=" + processInstancePriority
                + ", updateTime=" + updateTime
                + ", workerGroup='" + workerGroup + '\''
                + ", environmentCode='" + environmentCode + '\''
                + ", dryRun='" + dryRun + '\''
                + ", processInstanceId='" + processInstanceId + '\''
                + ", processDefinitionVersion='" + processDefinitionVersion + '\''
                + ", depDataNames='" + depDataNames + '\''
                + ", depDataTimeReplacedName='" + depDataTimeReplacedName + '\''
                + ", onlineFlag='" + onlineFlag + '\''
                + '}';
    }

}

