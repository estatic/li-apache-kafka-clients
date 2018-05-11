/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.largemessage;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroSegmentSerializer implements Serializer<LargeMessageSegment> {
    private final int CHECKSUM_LENGTH = Integer.BYTES;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, LargeMessageSegment segment) {
        if (segment.numberOfSegments > 1) {
            String schema = "{\"namespace\": \"avro-message.avro\",\n" +
                    " \"type\": \"record\",\n" +
                    " \"name\": \"LargeAvroMessage\",\n" +
                    " \"fields\": [\n" +
                    "     {\"name\": \"key\", \"type\": \"string\"},\n" +
                    "     {\"name\": \"value\",  \"type\": [\"byte\"]},\n" +
                    "     {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}\n" +
                    "}";
            Schema sc = new Schema.Parser().parse(schema);
            LargeAvroMessage data = new LargeAvroMessage();
            Map<String, Object> metadata = new HashMap<>();
            data.setKey(s);
            data.setValue(segment.payload);

            metadata.put("version", LargeMessageSegment.CURRENT_VERSION);
            metadata.put("sign", (segment.messageId.getMostSignificantBits() + segment.messageId.getLeastSignificantBits()));
            metadata.put("most_sign_bits", segment.messageId.getMostSignificantBits());
            metadata.put("least_sign_bits", segment.messageId.getLeastSignificantBits());
            metadata.put("sequence_number", segment.sequenceNumber);

            metadata.put("number_of_segments", segment.numberOfSegments);
            metadata.put("message_size_in_bytes", segment.messageSizeInBytes);

            data.setMetadata(metadata);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            DatumWriter<LargeAvroMessage> writer = new SpecificDatumWriter<>(sc);

            try {
                writer.write(data, encoder);
                encoder.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return out.toByteArray();
        } else {
            return segment.payloadArray();
        }
    }

    @Override
    public void close() {

    }
}
