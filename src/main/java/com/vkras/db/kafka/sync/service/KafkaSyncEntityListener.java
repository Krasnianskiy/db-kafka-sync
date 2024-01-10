package com.vkras.db.kafka.sync.service;

import com.vkras.db.kafka.sync.annotation.SynchronizedTable;
import com.vkras.db.kafka.sync.annotation.handlers.Converter;
import com.vkras.db.kafka.sync.config.SynchronizedProperties;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.persister.entity.EntityPersister;

public class KafkaSyncEntityListener implements PostUpdateEventListener {

    private final SynchronizedProperties properties;

    private final KafkaProducer<String, Object> kafkaProducer;
    public KafkaSyncEntityListener(SynchronizedProperties synchronizedProperties,
                                   KafkaProducer<String, Object> kafkaProducer) {
        this.properties = synchronizedProperties;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onPostUpdate(PostUpdateEvent postUpdateEvent) {
        Object entity = postUpdateEvent.getEntity();
        Class<?> type = entity.getClass();
        if (type.isAnnotationPresent(Entity.class) && type.isAnnotationPresent(SynchronizedTable.class)){
            // table name as a topic
            String tableName = type.getAnnotation(Table.class).name();
            try {
                Converter converter = type.getAnnotation(SynchronizedTable.class).converter().newInstance();
                Object resultEntity = converter.convert(entity);
                ProducerRecord<String, Object> record = new ProducerRecord<>(tableName, resultEntity);
                kafkaProducer.send(record);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean requiresPostCommitHandling(EntityPersister entityPersister) {
        return false;
    }


}
