package com.vkras.db.kafka.sync.entity;

import com.vkras.db.kafka.sync.utils.ConsumerStatus;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "kafka_db_sync")
@Data
public class KafkaDBOffsetEntity {
    @Id
    private String tableName;

    private String offset;

    private ConsumerStatus status;

    private String errorMessage;
}
