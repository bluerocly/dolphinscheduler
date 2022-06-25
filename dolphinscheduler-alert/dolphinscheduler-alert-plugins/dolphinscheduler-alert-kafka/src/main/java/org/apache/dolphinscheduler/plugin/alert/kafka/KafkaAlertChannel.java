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

package org.apache.dolphinscheduler.plugin.alert.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertInfo;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import com.google.common.base.Preconditions;

public final class KafkaAlertChannel implements AlertChannel {
	private static Map<String,KafkaSender> kafkaSenderMap = new HashMap<>();
	
    @Override
    public AlertResult process(AlertInfo alertInfo) {
        AlertData alertData = alertInfo.getAlertData();
        Map<String, String> alertParams = alertInfo.getAlertParams();
        if (alertParams == null || alertParams.size() == 0) {
            return new AlertResult("false", "Kafka alert params is empty");
        }
        
        // TODO 每发一次消息 都要new个producer实例？也没有close？
        KafkaSender kafkaSender = getKafkaSender(alertParams);
        if(StringUtils.isNotEmpty(alertData.getTitle())) {
        	return kafkaSender.sendMessage(alertData.getTitle(),alertData.getContent());
        } else {
        	return kafkaSender.sendMessage(alertData.getContent());
        }
    }
    
	private KafkaSender getKafkaSender(Map<String, String> kafkaAlertParam) {
		String servers = kafkaAlertParam.get(KafkaParamsConstants.KAFKA_SERVERS_NAME);
        String topic = kafkaAlertParam.get(KafkaParamsConstants.KAFKA_TOPIC_NAME);
        Preconditions.checkArgument(!Objects.isNull(servers), "KakfaServers can not be null");
        Preconditions.checkArgument(!Objects.isNull(topic), "topic can not be null");
        String key = String.format("%s-%s", servers,topic);
        if(!kafkaSenderMap.containsKey(key)) {
        	KafkaSender kafkaSender = new KafkaSender(kafkaAlertParam);
        	kafkaSenderMap.put(key, kafkaSender);
        }
        return kafkaSenderMap.get(key);
	}
}
