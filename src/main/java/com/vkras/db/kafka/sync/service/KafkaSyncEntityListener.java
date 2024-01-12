package com.vkras.db.kafka.sync.service;

import com.vkras.db.kafka.sync.annotation.SynchronizedTable;
import com.vkras.db.kafka.sync.config.SynchronizedProperties;
import jakarta.persistence.Entity;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.hibernate.persister.entity.EntityPersister;

import java.util.Objects;

@Slf4j
public class KafkaSyncEntityListener implements PostUpdateEventListener {

    private final SynchronizedProperties properties;

    private final DbKafkaProducer kafkaProducer;
    public KafkaSyncEntityListener(SynchronizedProperties synchronizedProperties,
                                   DbKafkaProducer kafkaProducer) {
        this.properties = synchronizedProperties;
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * Checking if saving entity has {@link SynchronizedTable} annotation
     * if it passed then sending message with this entity to kafka
     * if sending the message failed, then rollback transaction
     * @param postUpdateEvent - Hibernate update event
     */
    @Override
    @SuppressWarnings("unchecked")
    public void onPostUpdate(PostUpdateEvent postUpdateEvent) {
        Object entity = postUpdateEvent.getEntity();
        Class<?> type = entity.getClass();
        if (type.isAnnotationPresent(Entity.class) && type.isAnnotationPresent(SynchronizedTable.class)){
            kafkaProducer.sendSyncMessage(entity, (recordMetadata, e) -> {
                if (Objects.nonNull(e)){
                    // During the producer error - rollback data save
                    log.error("Error sending message to Kafka, \n Partition: {}, \n Error: {}",
                            recordMetadata.partition(), e.getMessage());
                    postUpdateEvent.getSession().getTransaction().rollback();
                }
            });
            postUpdateEvent.getSession().getTransaction().rollback();
        }
    }

    @Override
    public boolean requiresPostCommitHandling(EntityPersister entityPersister) {
        return false;
    }


}
