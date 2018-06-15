/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */
package com.linkedin.kafka.clients.largemessage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AvroSegmentDeserializer implements Deserializer<LargeMessageSegment> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSegmentDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public LargeMessageSegment deserialize(String s, byte[] bytes) {

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
        GenericData.Record data1 = new GenericData.Record(sc);

        DatumReader<GenericData.Record> reader = new SpecificDatumReader<>(sc);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        try {
            GenericData.Record data = reader.read(null, decoder);
            Map<Utf8, Utf8> metadataUtf = (Map<Utf8, Utf8>) data.get("metadata");
            Map<String, String> metadata = new HashMap<>();
            metadataUtf.forEach((k, v) -> metadata.put(k.toString(), v.toString()));

            byte version = Byte.valueOf(metadata.get("version"));
            if (version > LargeMessageSegment.CURRENT_VERSION) {
                LOG.debug("Serialized version byte is greater than {}. not large message segment.",
                        LargeMessageSegment.CURRENT_VERSION);
                return null;
            }
            long checksum = Long.valueOf(metadata.get("sign"));
            long messageIdMostSignificantBits = Long.valueOf(metadata.get("most_sign_bits"));
            long messageIdLeastSignificantBits = Long.valueOf(metadata.get("least_sign_bits"));
            if (checksum == 0 ||
                    checksum != (messageIdMostSignificantBits + messageIdLeastSignificantBits)) {
                LOG.debug("Serialized segment checksum does not match. not large message segment.");
                return null;
            }
            UUID messageId = new UUID(messageIdMostSignificantBits, messageIdLeastSignificantBits);
            ByteBuffer payload = (ByteBuffer) data.get("value");
            return new LargeMessageSegment(messageId,
                    Integer.valueOf(metadata.get("sequence_number")),
                    Integer.valueOf(metadata.get("number_of_segments")),
                    Integer.valueOf(metadata.get("message_size_in_bytes")), payload);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
