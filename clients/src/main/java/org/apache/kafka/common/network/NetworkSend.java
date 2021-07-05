/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.IOException;

/**
 * 内部包含了与channel交互的逻辑 以及数据包
 */
public class NetworkSend implements Send {
    private final String destinationId;
    private final Send send;

    public NetworkSend(String destinationId, Send send) {
        this.destinationId = destinationId;
        this.send = send;
    }

    public String destinationId() {
        return destinationId;
    }

    @Override
    public boolean completed() {
        return send.completed();
    }

    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        return send.writeTo(channel);
    }

    @Override
    public long size() {
        return send.size();
    }

}
