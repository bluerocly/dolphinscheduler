package org.apache.dolphinscheduler.server.master.consumer.message;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.CommandPush;
import org.apache.dolphinscheduler.dao.entity.CommandPushWaiting;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageCommandConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MessageCommandConsumer.class);
    
    public static final String header_msg_type = "message_type";
    public static final String msg_data_name = "kpiGroupName";
    public static final String msg_data_time = "dataTime";
    public static final String msg_data_period = "dataPeriod";
    public static final String body_msg_type = "msgtype";
    
    /**
     * processService
     */
    @Autowired
    private ProcessService processService;
    
    @KafkaListener(topics = "${master.message.command.topicName:}")
	public void onMessage(ConsumerRecord<String, String> record) {
		try {
			Map<String, String> headerMap = new HashMap<>();
			Headers headers = record.headers();
			for (Header header : headers) {
				String key = header.key();
				String value = new String(header.value());
				headerMap.put(key, value);
			}
			String msg_type = headerMap.get(header_msg_type);
			int partitionId = record.partition();
			long offset = record.offset();
			String body = record.value();
			if(StringUtils.isEmpty(msg_type) ) {
				msg_type = JSONUtils.getNodeStringNoDoubleQuotes(body, body_msg_type);	
			}
			MessageTypeEnum msgType = MessageTypeEnum.getMessageTypeEnum(msg_type);
			Set<String> dataNameSet = new HashSet<>();
			String dataTimeStr = JSONUtils.getNodeStringNoDoubleQuotes(body, msg_data_time);
			String dataPeriodStr = JSONUtils.getNodeStringNoDoubleQuotes(body, msg_data_period);	
	        switch (msgType) {
	        case dbinsert_message:
	        	dataNameSet = getDataNameSetDbInsert(body);
	        	break;
	        case dbintegrity_message:
	        	dataNameSet = getDataNameSetDbIntegrity(body);
	        	break;
	        default:
	        	logger.info("unkown");
	        	return;
	        }
//			RawMessage message = new RawMessage();
//			message.setMsgType(msgType);
//			message.setData(record.value());
//			ActorRef actor = getActor(0);
//			actor.tell(message, null);
			logger.info("receive commandpush message, msg_type[{}], partition[{}], offset[{}], body:[{}] ", msg_type, partitionId, offset, body);
			
	    	Date dataTimeDate = DateUtils.stringToDate(dataTimeStr);
        	for(String dataName : dataNameSet) {
        		int update = processService.updateCommandPushWaitingReceiveFlagByDataNameAndDataTime(dataName,dataTimeDate);
                if(update < 1) {
                	List<CommandPush> commandPushList = processService.queryCommandPushListByDepDataName(dataName);
                	if(CollectionUtils.isNotEmpty(commandPushList)) {
                		for(CommandPush commandPush : commandPushList) {
                			String[] splits = commandPush.getDepDataNames().split(",");
                			List<CommandPushWaiting> commandPushWaitingList = new ArrayList<>();
                			for(String split : splits) {
                				if(StringUtils.isNotEmpty(split)) {
                					CommandPushWaiting commandPushWaiting = new CommandPushWaiting();
                					commandPushWaiting.setProcessDefinitionCode(commandPush.getProcessDefinitionCode());
                					commandPushWaiting.setCommandPushId(commandPush.getId());
                					commandPushWaiting.setDepDataName(split);
                					commandPushWaiting.setDepDataTime(dataTimeDate);
                					if(StringUtils.isNotEmpty(dataPeriodStr)) {
                						commandPushWaiting.setDepDataPeriod(Integer.valueOf(dataPeriodStr));
                					}
                					commandPushWaiting.setInsertTime(new Date());
                					commandPushWaiting.setUpdateTime(new Date());
                					if(split.equalsIgnoreCase(dataName)) {
                						commandPushWaiting.setReceiveFlag(1);
//                						sqlList.add("insert into t_ds_command_push_waiting(process_definition_code,command_push_id, dep_data_name, dep_data_time, receive_flag) values(?,?,?,?,1))");        						
                					} else {
                						commandPushWaiting.setReceiveFlag(0);
                					}
                					commandPushWaitingList.add(commandPushWaiting);
                				}
                			}
                			int insertCount = processService.createCommandPushWaitings(commandPushWaitingList);
                			logger.info("depDataName[{}], getProcessDefinitionCode[{}], depDataNames[{}], createCommandPushWaitings[{}], commandPushWaitingList.size[{}]", dataName, commandPush.getProcessDefinitionCode(), commandPush.getDepDataNames(), insertCount, commandPushWaitingList.size());
                		}
                	}
                }
        	}
        	
		} catch (Exception e) {
			logger.error("process get error:", e);
		}
	}

	private Set<String> getDataNameSetDbIntegrity(String body) {
		Set<String> hs = new HashSet<>();
		String kpiGroupName = JSONUtils.getNodeStringNoDoubleQuotes(body, "kpiGroupName");
		hs.add(kpiGroupName);
		return hs;
	}

	private Set<String> getDataNameSetDbInsert(String body) {
		Set<String> hs = new HashSet<>();
    	String recordsJsonStr = JSONUtils.getNodeStringNoDoubleQuotes(body, "records");
    	if(StringUtils.isNotEmpty(recordsJsonStr)) {
    		List<HashMap> mapList = JSONUtils.toList(recordsJsonStr, HashMap.class);
    		for(HashMap hashMap : mapList) {
    			Object dataNameObj = hashMap.get("kpiGroupName");
    			if(dataNameObj != null) {
    				hs.add((String) dataNameObj);
    			}
    		}
    	}
		return hs;
	}

}
