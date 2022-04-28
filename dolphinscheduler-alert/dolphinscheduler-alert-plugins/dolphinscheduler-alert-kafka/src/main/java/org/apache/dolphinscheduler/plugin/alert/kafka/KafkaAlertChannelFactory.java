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

import org.apache.dolphinscheduler.alert.api.AlertChannel;
import org.apache.dolphinscheduler.alert.api.AlertChannelFactory;
import org.apache.dolphinscheduler.spi.params.base.PluginParams;
import org.apache.dolphinscheduler.spi.params.base.Validate;
import org.apache.dolphinscheduler.spi.params.input.InputParam;

import java.util.LinkedList;
import java.util.List;

import com.google.auto.service.AutoService;

@AutoService(AlertChannelFactory.class)
public final class KafkaAlertChannelFactory implements AlertChannelFactory {
    @Override
    public String name() {
        return "Kafka";
    }

    @Override
    public List<PluginParams> params() {
        List<PluginParams> paramsList = new LinkedList<>();

        InputParam kafkaParam = InputParam.newBuilder(KafkaParamsConstants.KAFKA_SERVERS_NAME, KafkaParamsConstants.KAFKA_SERVERS)
                                            .addValidate(Validate.newBuilder()
                                                                 .setRequired(true)
                                                                 .build())
                                            .setPlaceholder("Input Servers, likes host1:port1,host2:port2")
                                            .build();

        InputParam topic = InputParam.newBuilder(KafkaParamsConstants.KAFKA_TOPIC_NAME, KafkaParamsConstants.KAFKA_TOPIC)
                                       .addValidate(Validate.newBuilder()
                                                            .setRequired(true)
                                                            .build())
                                       .setPlaceholder("Input the topic")
                                       .build();

        paramsList.add(kafkaParam);
        paramsList.add(topic);
        return paramsList;
    }

    @Override
    public AlertChannel create() {
        return new KafkaAlertChannel();
    }
}
