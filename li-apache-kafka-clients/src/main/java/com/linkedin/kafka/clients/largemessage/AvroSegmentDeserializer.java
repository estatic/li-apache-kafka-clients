package com.linkedin.kafka.clients.largemessage;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class AvroSegmentDeserializer implements Deserializer<LargeMessageSegment> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSegmentDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public LargeMessageSegment deserialize(String s, byte[] bytes) {

        DatumReader<LargeAvroMessage> reader = new SpecificDatumReader<>(LargeAvroMessage.class);

        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        try {
            LargeAvroMessage data = reader.read(null, decoder);
            byte version = (byte)data.getMetadata().get("version");
            if (version > LargeMessageSegment.CURRENT_VERSION) {
                LOG.debug("Serialized version byte is greater than {}. not large message segment.",
                        LargeMessageSegment.CURRENT_VERSION);
                return null;
            }
            int checksum = (int)data.getMetadata().get("sign");
            long messageIdMostSignificantBits = (long)data.getMetadata().get("most_sign_bits");
            long messageIdLeastSignificantBits = (long)data.getMetadata().get("least_sign_bits");
            if (checksum == 0 ||
                    checksum != ((int) (messageIdMostSignificantBits + messageIdLeastSignificantBits))) {
                LOG.debug("Serialized segment checksum does not match. not large message segment.");
                return null;
            }
            UUID messageId = new UUID(messageIdMostSignificantBits, messageIdLeastSignificantBits);
            return new LargeMessageSegment(messageId,
                    (int)data.getMetadata().get("sequence_number"),
                    (int)data.getMetadata().get("number_of_segments"),
                    (int)data.getMetadata().get("message_size_in_bytes"), data.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
