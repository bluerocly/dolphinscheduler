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

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

public final class KafkaSender {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(KafkaSender.class);

    private final String servers;
    private final String topic;

	private KafkaProducer<String, String> producer;

    public KafkaSender(Map<String, String> kafkaAlertParam) {
        servers = kafkaAlertParam.get(KafkaParamsConstants.KAFKA_SERVERS_NAME);
        topic = kafkaAlertParam.get(KafkaParamsConstants.KAFKA_TOPIC_NAME);
        Preconditions.checkArgument(!Objects.isNull(servers), "KakfaServers can not be null");
//        Preconditions.checkArgument(webHookUrl.startsWith("https://hooks.slack.com/services/"), "SlackWebHookURL invalidate");
        Preconditions.checkArgument(!Objects.isNull(topic), "topic can not be null");
        
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        producer = new KafkaProducer<>(kafkaProperties);
        
    }
    
    public AlertResult sendMessage(String content) {
    	return sendMessage(this.topic,content);
    }

    /**
     * Send message to kafka channel
     *
     * @param title title
     * @param content content
     * @return AlertResult
     */
    public AlertResult sendMessage(String topic, String content) {
    	AlertResult alertResult = new AlertResult();
    	alertResult.setStatus("false");
    	try {
//			ProducerRecord<String, String> record = new ProducerRecord<>(mqTopic, jsonStr);
//			record.headers().add(MESSAGE_TYPE, MESSAGE_TYPE_VALUE.getBytes(StandardCharsets.UTF_8));
    		Future<RecordMetadata> send = producer.send(new ProducerRecord<>(topic, content));
    		producer.flush();
//    		long offset = send.get().offset();
//    		alertResult.setMessage(offset+"");
    	    alertResult.setStatus("true");
    	    log.info(String.format("Send message to kafka success, servers[%s], topic[%s], content[%s].", this.servers, topic, content));
            return alertResult;
        } catch (Exception e) {
            log.error(String.format("Send message to kafka error, servers[%s], topic[%s], content[%s].", this.servers, topic, content), e);
            alertResult.setMessage("System Exception");
            return alertResult;
        }
    }

	public void close() {
		if(producer != null) {
			producer.close();
		}
	}
}
