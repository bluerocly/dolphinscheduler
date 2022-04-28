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

import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.junit.Assert;
import org.junit.Test;

public class KafkaSenderTest {

    @Test
    public void testSendMessage() {
        Map<String, String> alertparam = new HashMap<>();
        alertparam.put(KafkaParamsConstants.KAFKA_SERVERS_NAME,
            "tongzt-71:6667");
        alertparam.put(KafkaParamsConstants.KAFKA_TOPIC_NAME, "tongcstest");

        KafkaSender kafkaSender = new KafkaSender(alertparam);
        AlertResult response = kafkaSender.sendMessage("test content");
        System.out.println(response.getMessage());
        Assert.assertEquals("true", response.getStatus());
    }
}
