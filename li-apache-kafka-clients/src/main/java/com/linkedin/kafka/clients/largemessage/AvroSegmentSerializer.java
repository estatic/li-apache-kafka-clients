/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.largemessage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
        String schema = "{\"namespace\": \"message.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"GenericData.Record\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"key\", \"type\": [\"null\", \"string\"]},\n" +
            "     {\"name\": \"value\",  \"type\": [\"bytes\"]},\n" +
            "     {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": [\"null\", \"string\"]}}" +
            "   ]\n" +
            "}";
        Schema sc = new Schema.Parser().parse(schema);
        GenericData.Record data = new GenericData.Record(sc);
        Map<String, String> metadata = new HashMap<>();
        data.put("key", s == null ? String.valueOf(segment.messageId.getMostSignificantBits()) : s);
        data.put("value", segment.payload);

        metadata.put("version", String.valueOf(LargeMessageSegment.CURRENT_VERSION));
        long checksum = segment.messageId.getMostSignificantBits() + segment.messageId.getLeastSignificantBits();
        metadata.put("sign", String.valueOf(checksum));
        metadata.put("most_sign_bits", String.valueOf(segment.messageId.getMostSignificantBits()));
        metadata.put("least_sign_bits", String.valueOf(segment.messageId.getLeastSignificantBits()));
        metadata.put("sequence_number", String.valueOf(segment.sequenceNumber));

        metadata.put("number_of_segments", String.valueOf(segment.numberOfSegments));
        metadata.put("message_size_in_bytes", String.valueOf(segment.messageSizeInBytes));
        data.put("metadata", metadata);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(sc);

        try {
            writer.write(data, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    @Override
    public void close() {

    }
}
