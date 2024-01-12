package com.vkras.db.kafka.sync.service;

import com.vkras.db.kafka.sync.annotation.ExternalTable;
import com.vkras.db.kafka.sync.entity.KafkaDBOffsetEntity;
import com.vkras.db.kafka.sync.repository.KafkaOffsetRepository;
import com.vkras.db.kafka.sync.utils.ScanUtil;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.vkras.db.kafka.sync.utils.Constants.SYNC_TOPIC;


/**
 * This class provides the message for full sync of tables
 */
@AllArgsConstructor
@SuppressWarnings("unchecked")
public class SyncTablesService implements SyncTables{
    private final ScanUtil scanUtil;
    private final KafkaOffsetRepository repository;

    private final KafkaProducer kafkaProducer;


    @Override
    public void syncAllTables(){
        List<Class<?>> entites = scanUtil.findClasses(ExternalTable.class);
        List<String> tables = entites.stream()
                .map(aClass -> aClass.getAnnotation(Table.class).name())
                .collect(Collectors.toList());
        List<String> emptyTables = repository.findAllById(tables)
                .stream()
                .map(KafkaDBOffsetEntity::getTableName)
                .filter(tables::contains)
                .collect(Collectors.toList());
        sendMessage(emptyTables);
    }

    @Override
    public void syncTable(String tableName){
        sendMessage(Collections.singletonList(tableName));
    }

    /**
     * sending the message to kafka
     * @param tableNames - names of tables
     */
    private void sendMessage(List<String> tableNames){
        ProducerRecord<String, List<String>> producerRecord = new ProducerRecord<>(SYNC_TOPIC, tableNames);
        kafkaProducer.send(producerRecord);
    }
}
