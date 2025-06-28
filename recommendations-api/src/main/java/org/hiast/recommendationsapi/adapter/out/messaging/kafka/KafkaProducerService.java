package org.hiast.recommendationsapi.adapter.out.messaging.kafka;

import org.apache.fury.Fury;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hiast.model.InteractionEvent;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class KafkaProducerService {
    private static final String KAFKA_BROKER = "localhost:9094";
    private static final String INPUT_TOPIC = "user_interactions";
    private final Producer<String, byte[]> producer;
    private final Fury fury;

    public KafkaProducerService() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
        this.fury = FurySerializationUtils.createConfiguredFury();
    }

    public void sendInteractionEvent(InteractionEvent event) {
        byte[] serializedEvent = fury.serialize(event);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC, null, serializedEvent);
        producer.send(record);
    }
} 