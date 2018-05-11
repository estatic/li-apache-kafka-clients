/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.largemessage;

import java.nio.ByteBuffer;
import java.util.Map;

public class LargeAvroMessage {

    private String key;
    private ByteBuffer value;
    private Map<String, Object> metadata;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
