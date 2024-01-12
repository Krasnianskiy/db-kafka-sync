package com.vkras.db.kafka.sync.service;

import com.vkras.db.kafka.sync.annotation.SynchronizedTable;
import com.vkras.db.kafka.sync.annotation.handlers.Converter;
import jakarta.persistence.Table;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class DbKafkaProducer extends KafkaProducer {
    public DbKafkaProducer(Properties properties) {
        super(properties);
    }

    /**
     * Getting topic name by table name
     * Converting Entity to Mapped type
     * Sending message to Kafka
     * @param entity - {@link jakarta.persistence.Entity} / {@link List} of {@link jakarta.persistence.Entity}
     * @param callback - producer callback
     */
    public void sendSyncMessage(Object entity, Callback callback) {
        Class<?> type;
        if (entity instanceof List){
            type = entity.getClass().getComponentType();;
        }
        else {
            type = entity.getClass();
        }
        String tableName = type.getAnnotation(Table.class).name();
        try {
            Object resultEntity;
            Converter converter = type.getAnnotation(SynchronizedTable.class).converter().newInstance();
            if (entity instanceof List){
                List<Object> listEntities = (List<Object>) entity;
                resultEntity = listEntities.stream()
                        .map(o -> converter.convert(o))
                        .collect(Collectors.toList());
            }
            else {
                resultEntity = converter.convert(entity);
            }
            ProducerRecord<String, Object> record = new ProducerRecord<>(tableName, resultEntity);
            send(record, callback);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
