package com.vkras.db.kafka.sync.repository;

import com.vkras.db.kafka.sync.entity.KafkaDBOffsetEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOffsetRepository extends JpaRepository<KafkaDBOffsetEntity, String> {
}
