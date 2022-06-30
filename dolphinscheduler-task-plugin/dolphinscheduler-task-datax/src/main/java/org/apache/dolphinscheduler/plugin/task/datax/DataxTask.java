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

import static org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils.decodePassword;
import static org.apache.dolphinscheduler.spi.task.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.spi.task.TaskConstants.RWXR_XR_X;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.ftp.FtpConnectionParam;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DatasourceUtil;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.ShellCommandExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.util.MapUtils;
import org.apache.dolphinscheduler.plugin.task.util.OSUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DataType;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.enums.Flag;
import org.apache.dolphinscheduler.spi.task.AbstractParameters;
import org.apache.dolphinscheduler.spi.task.Direct;
import org.apache.dolphinscheduler.spi.task.Property;
import org.apache.dolphinscheduler.spi.task.TaskAlertInfo;
import org.apache.dolphinscheduler.spi.task.TaskConstants;
import org.apache.dolphinscheduler.spi.task.paramparser.ParamUtils;
import org.apache.dolphinscheduler.spi.task.paramparser.ParameterUtils;
import org.apache.dolphinscheduler.spi.task.request.DataxTaskExecutionContext;
import org.apache.dolphinscheduler.spi.task.request.TaskRequest;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DataxTask extends AbstractTaskExecutor {
    /**
     * jvm parameters
     */
    public static final String JVM_PARAM = " --jvm=\"-Xms%sG -Xmx%sG\" ";
    /**
     * python process(datax only supports version 2.7 by default)
     */
//    private static final String DATAX_PYTHON = "python2.7";
    private static final String DATAX_PYTHON = "python";
    private static final Pattern PYTHON_PATH_PATTERN = Pattern.compile("/bin/python[\\d.]*$");
    /**
     * datax path
     */
    private static final String DATAX_PATH = "${DATAX_HOME}/bin/datax.py";
    private static final String DATAX_PATH_WIN = "%DATAX_HOME%/bin/datax.py";
    
    private static final String S_ROW_NUM = "s_row_num";
    private static final String S_FILE_SIZE = "s_file_size";
    private static final String S_UUID = "s_uuid";
    private static final String S_FTP_INFO = "s_ftp_info";
    
    /**
     * datax channel count
     */
    private static final int DATAX_CHANNEL_COUNT = 1;

    /**
     * datax parameters
     */
    private DataxParameters dataXParameters;

    /**
     * shell command executor
     */
    private ShellCommandExecutor shellCommandExecutor;

    /**
     * taskExecutionContext
     */
    private TaskRequest taskExecutionContext;

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    public DataxTask(TaskRequest taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;

        this.shellCommandExecutor = new ShellCommandExecutor(this::logHandle,
                taskExecutionContext, logger);
    }

    /**
     * init DataX config
     */
    @Override
    public void init() {
        logger.info("datax task params {}", taskExecutionContext.getTaskParams());
        dataXParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), DataxParameters.class);

        if (!dataXParameters.checkParameters()) {
            throw new RuntimeException("datax task params is not valid");
        }
    }

    /**
     * run DataX process
     *
     * @throws Exception if error throws Exception
     */
    @Override
    public void handle() throws Exception {
        try {
        	String result = null;
            // replace placeholder,and combine local and global parameters
            Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext, getParameters());
            if (MapUtils.isEmpty(paramsMap)) {
                paramsMap = new HashMap<>();
            }
            if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
                paramsMap.putAll(taskExecutionContext.getParamsMap());
            }

            // run datax procesDataSourceService.s
            String jsonFilePath = buildDataxJsonFile(paramsMap);
            String shellCommandFilePath = buildShellCommandFile(jsonFilePath, paramsMap);
            TaskResponse commandExecuteResult = shellCommandExecutor.run(shellCommandFilePath);

            setExitStatusCode(commandExecuteResult.getExitStatusCode());
            setAppIds(commandExecuteResult.getAppIds());
            setProcessId(commandExecuteResult.getProcessId());
            
            // 增加日志解析获取指标？还是发
            // 发送通知消息
            String varPool = shellCommandExecutor.getVarPool();
            Map<String, String> mapByString = DataxParameters.getMapByString(varPool);
            long readerNum = Long.valueOf(mapByString.getOrDefault(TaskConstants.TASK_RECORD_READER_NUM, "0"));
//            	int errorNum = Integer.valueOf(mapByString.getOrDefault(TaskConstants.TASK_RECORD_WRITING_ERROR_NUM, "0"));
//            	int avgFlow = Integer.valueOf(mapByString.getOrDefault(TaskConstants.TASK_AVERAGE_FLOW, "0").replace("B/s", ""));
//            	int totalTime = Integer.valueOf(mapByString.getOrDefault(TaskConstants.TASK_TOTAL_TIME, "0").replace("s", ""));
            long writeNum = Long.valueOf(mapByString.getOrDefault(TaskConstants.TASK_RECORD_WRITING_NUM, "0"));
            long writeSize = Long.valueOf(mapByString.getOrDefault(TaskConstants.TASK_RECORD_WRITING_BYTES, "0"));
            if(writeNum == readerNum+1) {
            	writeNum = readerNum;
            }
//            writeNum = 100; // by win test
            if(writeNum == 0) {
            	logger.error("ftpwriter's write num is 0. please check the flow's data.");
            	setExitStatusCode(EXIT_CODE_FAILURE);
//            	throw new Exception("ftpwriter's write num is 0. please check the flow.");
            }
            result = setDataxNonQuerySqlReturn("" + writeNum, dataXParameters.getLocalParams());
            //使用添加datax默认outParameter参数，可实现，因为sql out参数是开放出去的，所以sql中增加了一下逻辑
//            if(dataXParameters.getVarPool() != null) {
//            	dataXParameters.getVarPool().add(new Property(Constants.TASK_DATA_COUNT, Direct.OUT, DataType.VARCHAR, ""+writeNum));
//            	logger.info("add taskExecuteCount[{}] to varpool", writeNum);
//            }
            if(dataXParameters.getNotification()==null || dataXParameters.getNotification()) {
            	String topicName = dataXParameters.getQueueName();
            	String msgContent = dataXParameters.getMessagejson();
            	
            	DataxTaskExecutionContext dataxTaskExecutionContext = taskExecutionContext.getDataxTaskExecutionContext();
            	FtpConnectionParam dataTargetCfg = (FtpConnectionParam) DatasourceUtil.buildConnectionParams(
                        DbType.of(dataxTaskExecutionContext.getTargetType()),
                        dataxTaskExecutionContext.getTargetConnectionParams());
                String address = dataTargetCfg.getAddress();
            	Map<String, String> convertMap = ParamUtils.convert(paramsMap);
            	convertMap.put(S_ROW_NUM, ""+writeNum);
            	convertMap.put(S_FILE_SIZE, ""+writeSize);
            	convertMap.put(S_UUID, UUID.randomUUID().toString());
            	convertMap.put(S_FTP_INFO, address);
                // replace placeholder
            	topicName = ParameterUtils.convertParameterPlaceholders(topicName, convertMap);
            	msgContent = ParameterUtils.convertParameterPlaceholders(msgContent, convertMap);
            	logger.info("send topicName[{}], msgContent [{}] to alert." , topicName, msgContent);
            	sendNotify(dataXParameters.getGroupId(), topicName, msgContent);
            }
            dataXParameters.dealOutParam(result);
            removeInDirectTaskDataCount(dataXParameters.getVarPool());
        } catch (Exception e) {
            setExitStatusCode(EXIT_CODE_FAILURE);
            throw e;
        }
    }
    
    private void removeInDirectTaskDataCount(List<Property> varPool) {
    	if(CollectionUtils.isNotEmpty(varPool)) {
    		Iterator<Property> iterator = varPool.iterator();
    		while(iterator.hasNext()) {
    			Property tmp = iterator.next();
    			if(Direct.IN == tmp.getDirect() && Constants.TASK_DATA_COUNT.equalsIgnoreCase(tmp.getProp())) {
    				iterator.remove();
    			}
    		}
    	}
	}

	private String setDataxNonQuerySqlReturn(String updateResult, List<Property> properties) {
        String result = null;
        for (Property info : properties) {
            if (Direct.OUT == info.getDirect()) {
                List<Map<String, String>> updateRL = new ArrayList<>();
                Map<String, String> updateRM = new HashMap<>();
                updateRM.put(info.getProp(), updateResult);
                updateRL.add(updateRM);
                result = JSONUtils.toJsonString(updateRL);
                break;
            }
        }
        return result;
    }
    
    /**
     * send alert as an notify
     *
     * @param title title
     * @param content content
     */
    private void sendNotify(int groupId, String title, String content) {
        setNeedAlert(Boolean.TRUE);
        TaskAlertInfo taskAlertInfo = new TaskAlertInfo();
        taskAlertInfo.setAlertGroupId(groupId);
        taskAlertInfo.setContent(content);
        taskAlertInfo.setTitle(title);
        setTaskAlertInfo(taskAlertInfo);
    }

    /**
     * cancel DataX process
     *
     * @param cancelApplication cancelApplication
     * @throws Exception if error throws Exception
     */
    @Override
    public void cancelApplication(boolean cancelApplication)
            throws Exception {
        // cancel process
        shellCommandExecutor.cancelApplication();
    }

    /**
     * build datax configuration file
     *
     * @return datax json file name
     * @throws Exception if error throws Exception
     */
    private String buildDataxJsonFile(Map<String, Property> paramsMap)
            throws Exception {
        // generate json
        String fileName = String.format("%s/%s_job.json",
                taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId());
        String json;

        Path path = new File(fileName).toPath();
        if (Files.exists(path)) {
            return fileName;
        }

        if (dataXParameters.getCustomConfig() == Flag.YES.ordinal()) {
            json = dataXParameters.getJson().replaceAll("\\r\\n", "\n");
        } else {
            ObjectNode job = JSONUtils.createObjectNode();
            
            if(DbType.FTP.name().equalsIgnoreCase(dataXParameters.getDtType())) {
            	job.putArray("content").addAll(buildDataxJobContentJsonDB2Ftp());
            } else {
            	job.putArray("content").addAll(buildDataxJobContentJson());
            }
            job.set("setting", buildDataxJobSettingJson());

            ObjectNode root = JSONUtils.createObjectNode();
            root.set("job", job);
            root.set("core", buildDataxCoreJson());
            json = root.toString();
        }

        // replace placeholder
        json = ParameterUtils.convertParameterPlaceholders(json, ParamUtils.convert(paramsMap));

        logger.debug("datax job json : {}", json);

        // create datax json file
        FileUtils.writeStringToFile(new File(fileName), json, StandardCharsets.UTF_8);
        return fileName;
    }

    /**
     * build datax job config
     *
     * @return collection of datax job config JSONObject
     * @throws SQLException if error throws SQLException
     */
    private List<ObjectNode> buildDataxJobContentJson() {
        DataxTaskExecutionContext dataxTaskExecutionContext = taskExecutionContext.getDataxTaskExecutionContext();
        BaseConnectionParam dataSourceCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getSourcetype()),
                dataxTaskExecutionContext.getSourceConnectionParams());

        BaseConnectionParam dataTargetCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataxTaskExecutionContext.getTargetConnectionParams());

        List<ObjectNode> readerConnArr = new ArrayList<>();
        ObjectNode readerConn = JSONUtils.createObjectNode();

        ArrayNode sqlArr = readerConn.putArray("querySql");
        for (String sql : new String[]{dataXParameters.getSql()}) {
            sqlArr.add(sql);
        }

        ArrayNode urlArr = readerConn.putArray("jdbcUrl");
        urlArr.add(DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDsType()), dataSourceCfg));

        readerConnArr.add(readerConn);

        ObjectNode readerParam = JSONUtils.createObjectNode();
        readerParam.put("username", dataSourceCfg.getUser());
        readerParam.put("password", decodePassword(dataSourceCfg.getPassword()));
        readerParam.putArray("connection").addAll(readerConnArr);

        ObjectNode reader = JSONUtils.createObjectNode();
        reader.put("name", DataxUtils.getReaderPluginName(DbType.of(dataxTaskExecutionContext.getSourcetype())));
        reader.set("parameter", readerParam);

        List<ObjectNode> writerConnArr = new ArrayList<>();
        ObjectNode writerConn = JSONUtils.createObjectNode();
        ArrayNode tableArr = writerConn.putArray("table");
        tableArr.add(dataXParameters.getTargetTable());

        writerConn.put("jdbcUrl", DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDtType()), dataTargetCfg));
        writerConnArr.add(writerConn);

        ObjectNode writerParam = JSONUtils.createObjectNode();
        writerParam.put("username", dataTargetCfg.getUser());
        writerParam.put("password", decodePassword(dataTargetCfg.getPassword()));

        String[] columns = parsingSqlColumnNames(DbType.of(dataxTaskExecutionContext.getSourcetype()),
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataSourceCfg, dataXParameters.getSql());

        ArrayNode columnArr = writerParam.putArray("column");
        for (String column : columns) {
            columnArr.add(column);
        }
        writerParam.putArray("connection").addAll(writerConnArr);

        if (CollectionUtils.isNotEmpty(dataXParameters.getPreStatements())) {
            ArrayNode preSqlArr = writerParam.putArray("preSql");
            for (String preSql : dataXParameters.getPreStatements()) {
                preSqlArr.add(preSql);
            }

        }

        if (CollectionUtils.isNotEmpty(dataXParameters.getPostStatements())) {
            ArrayNode postSqlArr = writerParam.putArray("postSql");
            for (String postSql : dataXParameters.getPostStatements()) {
                postSqlArr.add(postSql);
            }
        }

        ObjectNode writer = JSONUtils.createObjectNode();
        writer.put("name", DataxUtils.getWriterPluginName(DbType.of(dataxTaskExecutionContext.getTargetType())));
        writer.set("parameter", writerParam);

        List<ObjectNode> contentList = new ArrayList<>();
        ObjectNode content = JSONUtils.createObjectNode();
        content.set("reader", reader);
        content.set("writer", writer);
        contentList.add(content);

        return contentList;
    }
    
    /**
     * build datax job config
     *
     * @return collection of datax job config JSONObject
     * @throws SQLException if error throws SQLException
     */
    private List<ObjectNode> buildDataxJobContentJsonDB2Ftp() {
        DataxTaskExecutionContext dataxTaskExecutionContext = taskExecutionContext.getDataxTaskExecutionContext();
        BaseConnectionParam dataSourceCfg = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getSourcetype()),
                dataxTaskExecutionContext.getSourceConnectionParams());

        FtpConnectionParam dataTargetCfg = (FtpConnectionParam) DatasourceUtil.buildConnectionParams(
                DbType.of(dataxTaskExecutionContext.getTargetType()),
                dataxTaskExecutionContext.getTargetConnectionParams());

        List<ObjectNode> readerConnArr = new ArrayList<>();
        ObjectNode readerConn = JSONUtils.createObjectNode();

        ArrayNode sqlArr = readerConn.putArray("querySql");
        for (String sql : new String[]{dataXParameters.getSql()}) {
            sqlArr.add(sql);
        }

        ArrayNode urlArr = readerConn.putArray("jdbcUrl");
        urlArr.add(DatasourceUtil.getJdbcUrl(DbType.valueOf(dataXParameters.getDsType()), dataSourceCfg));

        readerConnArr.add(readerConn);

        ObjectNode readerParam = JSONUtils.createObjectNode();
        readerParam.put("username", dataSourceCfg.getUser());
        readerParam.put("password", decodePassword(dataSourceCfg.getPassword()));
        readerParam.putArray("connection").addAll(readerConnArr);

        ObjectNode reader = JSONUtils.createObjectNode();
        reader.put("name", DataxUtils.getReaderPluginName(DbType.of(dataxTaskExecutionContext.getSourcetype())));
        reader.set("parameter", readerParam);


        String address = dataTargetCfg.getAddress();
        
        String protocol = address.split(Constants.COLON)[0];
        String[] hostPort = address.split(Constants.AT_SIGN);
        String[] hostPortArray = hostPort[hostPort.length - 1].split(Constants.COLON);
        String host = hostPortArray[0];
        String port = hostPortArray[1];
        String subdirectory = dataXParameters.getSubdirectory();
        String fileName = dataXParameters.getFileName();
        String fieldDelimiter = dataXParameters.getFieldDelimiter();
        if(StringUtils.isEmpty(fieldDelimiter)) {
        	fieldDelimiter = "|";
        }
        String ftpEncoding = dataXParameters.getFtpEncoding();
        if(StringUtils.isEmpty(ftpEncoding)) {
        	ftpEncoding = "UTF-8";
        }
        String ftpHeader = dataXParameters.getFtpHeader();
        String[] ftpHeaderColumns = null;
        if(StringUtils.isEmpty(ftpHeader)) {
        	ftpHeaderColumns = new String[0];
        } else if("*".equals(ftpHeader)) {
        	ftpHeaderColumns = parsingSqlColumnNames(DbType.of(dataxTaskExecutionContext.getSourcetype()),
                    DbType.of(dataxTaskExecutionContext.getTargetType()),
                    dataSourceCfg, dataXParameters.getSql());
        } else {
        	ftpHeaderColumns = ftpHeader.split(",");
        }
        String ftpFileSuffix = dataXParameters.getFtpFileSuffix();
        if(StringUtils.isEmpty(ftpFileSuffix)) {
        	ftpFileSuffix = ".csv";
        }
        if(Constants.FTP_FILE_SUFFIX_NONE.equalsIgnoreCase(ftpFileSuffix)) {
        	ftpFileSuffix = ""; //为了支持有些文件没有后缀名字
        }
        
        String ftpDateFormat = dataXParameters.getFtpDateFormat();
        if(StringUtils.isEmpty(ftpDateFormat)) {
        	ftpDateFormat = "yyyy-MM-dd HH:mm:ss";
        }
        
        ObjectNode writerParam = JSONUtils.createObjectNode();
        writerParam.put("protocol", protocol);
        writerParam.put("host", host);
        writerParam.put("port", port);
        writerParam.put("username", dataTargetCfg.getUser());
        writerParam.put("password", decodePassword(dataTargetCfg.getPassword()));
        writerParam.put("timeout", "60000");
        writerParam.put("connectPattern", "PASV");
        writerParam.put("path", subdirectory);
        writerParam.put("fileName", fileName);
        
        writerParam.put("writeMode", "truncateone");
        writerParam.put("fieldDelimiter",fieldDelimiter);
        writerParam.put("encoding", ftpEncoding);
        writerParam.put("nullFormat", "");
        writerParam.put("dateFormat", ftpDateFormat);
        writerParam.put("fileFormat", "csv");
        writerParam.put("suffix", ftpFileSuffix);
        
        ArrayNode headerArr = writerParam.putArray("header");
        for (String headerColumn : ftpHeaderColumns) {
        	headerArr.add(headerColumn);
        }

        ObjectNode writer = JSONUtils.createObjectNode();
        writer.put("name", DataxUtils.getWriterPluginName(DbType.of(dataxTaskExecutionContext.getTargetType())));
        writer.set("parameter", writerParam);

        List<ObjectNode> contentList = new ArrayList<>();
        ObjectNode content = JSONUtils.createObjectNode();
        content.set("reader", reader);
        content.set("writer", writer);
        contentList.add(content);

        return contentList;
    }

    /**
     * build datax setting config
     *
     * @return datax setting config JSONObject
     */
    private ObjectNode buildDataxJobSettingJson() {

        ObjectNode speed = JSONUtils.createObjectNode();

        speed.put("channel", DATAX_CHANNEL_COUNT);

        if (dataXParameters.getJobSpeedByte() > 0) {
            speed.put("byte", dataXParameters.getJobSpeedByte());
        }

        if (dataXParameters.getJobSpeedRecord() > 0) {
            speed.put("record", dataXParameters.getJobSpeedRecord());
        }

        ObjectNode errorLimit = JSONUtils.createObjectNode();
        errorLimit.put("record", 0);
        errorLimit.put("percentage", 0);

        ObjectNode setting = JSONUtils.createObjectNode();
        setting.set("speed", speed);
        setting.set("errorLimit", errorLimit);

        return setting;
    }

    private ObjectNode buildDataxCoreJson() {

        ObjectNode speed = JSONUtils.createObjectNode();
        speed.put("channel", DATAX_CHANNEL_COUNT);

        if (dataXParameters.getJobSpeedByte() > 0) {
            speed.put("byte", dataXParameters.getJobSpeedByte());
        }

        if (dataXParameters.getJobSpeedRecord() > 0) {
            speed.put("record", dataXParameters.getJobSpeedRecord());
        }

        ObjectNode channel = JSONUtils.createObjectNode();
        channel.set("speed", speed);

        ObjectNode transport = JSONUtils.createObjectNode();
        transport.set("channel", channel);

        ObjectNode core = JSONUtils.createObjectNode();
        core.set("transport", transport);

        return core;
    }

    /**
     * create command
     *
     * @return shell command file name
     * @throws Exception if error throws Exception
     */
    private String buildShellCommandFile(String jobConfigFilePath, Map<String, Property> paramsMap)
            throws Exception {
        // generate scripts
        String fileName = String.format("%s/%s_node.%s",
                taskExecutionContext.getExecutePath(),
                taskExecutionContext.getTaskAppId(),
                OSUtils.isWindows() ? "bat" : "sh");

        Path path = new File(fileName).toPath();

        if (Files.exists(path)) {
            return fileName;
        }

        // datax python command
        StringBuilder sbr = new StringBuilder();
        sbr.append(getPythonCommand());
        sbr.append(" ");
        sbr.append(OSUtils.isWindows() ? DATAX_PATH_WIN : DATAX_PATH);
        sbr.append(" ");
        sbr.append(loadJvmEnv(dataXParameters));
        sbr.append(jobConfigFilePath);

        // replace placeholder
        String dataxCommand = ParameterUtils.convertParameterPlaceholders(sbr.toString(), ParamUtils.convert(paramsMap));

        logger.debug("raw script : {}", dataxCommand);

        // create shell command file
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString(RWXR_XR_X);
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);

        if (OSUtils.isWindows()) {
            Files.createFile(path);
        } else {
            Files.createFile(path, attr);
        }

        Files.write(path, dataxCommand.getBytes(), StandardOpenOption.APPEND);

        return fileName;
    }

    public String getPythonCommand() {
        String pythonHome = System.getenv("PYTHON_HOME");
        return getPythonCommand(pythonHome);
    }

    public String getPythonCommand(String pythonHome) {
        if (StringUtils.isEmpty(pythonHome)) {
            return DATAX_PYTHON;
        }
        String pythonBinPath = "/bin/" + DATAX_PYTHON;
        Matcher matcher = PYTHON_PATH_PATTERN.matcher(pythonHome);
        if (matcher.find()) {
            return matcher.replaceAll(pythonBinPath);
        }
        return Paths.get(pythonHome, pythonBinPath).toString();
    }

    public String loadJvmEnv(DataxParameters dataXParameters) {
        int xms = Math.max(dataXParameters.getXms(), 1);
        int xmx = Math.max(dataXParameters.getXmx(), 1);
        return String.format(JVM_PARAM, xms, xmx);
    }

    /**
     * parsing synchronized column names in SQL statements
     *
     * @param sourceType the database type of the data source
     * @param targetType the database type of the data target
     * @param dataSourceCfg the database connection parameters of the data source
     * @param sql sql for data synchronization
     * @return Keyword converted column names
     */
    private String[] parsingSqlColumnNames(DbType sourceType, DbType targetType, BaseConnectionParam dataSourceCfg, String sql) {
        String[] columnNames = tryGrammaticalAnalysisSqlColumnNames(sourceType, sql);

        if (columnNames == null || columnNames.length == 0) {
            logger.info("try to execute sql analysis query column name");
            columnNames = tryExecuteSqlResolveColumnNames(sourceType, dataSourceCfg, sql);
        }

        notNull(columnNames, String.format("parsing sql columns failed : %s", sql));

        return DataxUtils.convertKeywordsColumns(targetType, columnNames);
    }

    /**
     * try grammatical parsing column
     *
     * @param dbType database type
     * @param sql sql for data synchronization
     * @return column name array
     * @throws RuntimeException if error throws RuntimeException
     */
    private String[] tryGrammaticalAnalysisSqlColumnNames(DbType dbType, String sql) {
        String[] columnNames;

        try {
            SQLStatementParser parser = DataxUtils.getSqlStatementParser(dbType, sql);
            if (parser == null) {
                logger.warn("database driver [{}] is not support grammatical analysis sql", dbType);
                return new String[0];
            }

            SQLStatement sqlStatement = parser.parseStatement();
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
            SQLSelect sqlSelect = sqlSelectStatement.getSelect();

            List<SQLSelectItem> selectItemList = null;
            if (sqlSelect.getQuery() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) sqlSelect.getQuery();
                selectItemList = block.getSelectList();
            } else if (sqlSelect.getQuery() instanceof SQLUnionQuery) {
                SQLUnionQuery unionQuery = (SQLUnionQuery) sqlSelect.getQuery();
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) unionQuery.getRight();
                selectItemList = block.getSelectList();
            }

            notNull(selectItemList,
                    String.format("select query type [%s] is not support", sqlSelect.getQuery().toString()));

            columnNames = new String[selectItemList.size()];
            for (int i = 0; i < selectItemList.size(); i++) {
                SQLSelectItem item = selectItemList.get(i);

                String columnName = null;

                if (item.getAlias() != null) {
                    columnName = item.getAlias();
                } else if (item.getExpr() != null) {
                    if (item.getExpr() instanceof SQLPropertyExpr) {
                        SQLPropertyExpr expr = (SQLPropertyExpr) item.getExpr();
                        columnName = expr.getName();
                    } else if (item.getExpr() instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr expr = (SQLIdentifierExpr) item.getExpr();
                        columnName = expr.getName();
                    }
                } else {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item.toString()));
                }

                if (columnName == null) {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item.toString()));
                }

                columnNames[i] = columnName;
            }
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return new String[0];
        }

        return columnNames;
    }

    /**
     * try to execute sql to resolve column names
     *
     * @param baseDataSource the database connection parameters
     * @param sql sql for data synchronization
     * @return column name array
     */
    public String[] tryExecuteSqlResolveColumnNames(DbType sourceType, BaseConnectionParam baseDataSource, String sql) {
        String[] columnNames;
        sql = String.format("SELECT t.* FROM ( %s ) t WHERE 0 = 1", sql);
        sql = sql.replace(";", "");

        try (
                Connection connection = DataSourceClientProvider.getInstance().getConnection(sourceType, baseDataSource);
                PreparedStatement stmt = connection.prepareStatement(sql);
                ResultSet resultSet = stmt.executeQuery()) {

            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();
            columnNames = new String[num];
            for (int i = 1; i <= num; i++) {
                columnNames[i - 1] = md.getColumnName(i);
            }
        } catch (SQLException e) {
            logger.warn(e.getMessage(), e);
            return null;
        }

        return columnNames;
    }

    @Override
    public AbstractParameters getParameters() {
        return dataXParameters;
    }

    private void notNull(Object obj, String message) {
        if (obj == null) {
            throw new RuntimeException(message);
        }
    }

}
