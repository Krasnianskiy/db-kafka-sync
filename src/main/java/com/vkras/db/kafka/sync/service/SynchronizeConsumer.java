package com.vkras.db.kafka.sync.service;

import com.vkras.db.kafka.sync.annotation.ExternalTable;
import com.vkras.db.kafka.sync.repository.KafkaOffsetRepository;
import com.vkras.db.kafka.sync.utils.ScanUtil;
import jakarta.persistence.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.data.repository.CrudRepository;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class SynchronizeConsumer extends KafkaConsumer<Object, Object> {

    private final ScanUtil scanUtil;
    private final ApplicationContext ctx;

    private final KafkaOffsetRepository kafkaOffsetRepository;

    public SynchronizeConsumer(Properties properties,
                               ScanUtil scanUtil,
                               ApplicationContext ctx,
                               KafkaOffsetRepository kafkaOffsetRepository) {
        super(properties);
        this.scanUtil = scanUtil;
        this.ctx = ctx;
        this.kafkaOffsetRepository = kafkaOffsetRepository;
    }

    /**
     * Consumer for all entities annotated with {@link ExternalTable}
     * must have the table name annotated with {@link Table}
     * working with Spring Repositories
     * todo saving using JDBC template (if required/no repositories present)
     */
    @EventListener(ApplicationReadyEvent.class)
    @SuppressWarnings("unchecked")
    public void start() {
        subscribe(getTopicsNames());
        while (true) {
            ConsumerRecords<Object, Object> records = poll(1000);
            records.forEach(consumerRecord -> {
                Object value = consumerRecord.value();
                if (value instanceof List && !CollectionUtils.isEmpty((Collection<?>) value)) {
                    saveCollection(value);
                } else {
                    saveSingleObject(value);
                }
            });
        }
    }

    /**
     * Usual sync process
     * @param value - incoming Kafka Object
     */
    private void saveSingleObject(Object value) {
        CrudRepository repository = ctx.getBean(CrudRepository.class, value.getClass());
        try {
            Object result = convertValue(value);
            repository.save(result);
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("You don't have repository for class: " + value.getClass(), e);
        }
    }

    /**
     * The value could be {@link List} or {@link Object}
     * in the 1st case it's full Sync (1st run/api call)
     * in the 2nd case it's just real time sync
     * save batch sync
     * @param value - kafka incoming object
     */
    private void saveCollection(Object value){
        List<Object> result = ((List<?>) value)
                .stream()
                .map(val -> {
                    try {
                        return convertValue(val);
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        CrudRepository repository = ctx.getBean(CrudRepository.class, ((List<?>) value).get(0).getClass());
        repository.saveAll(result);
    }

    /**
     * Using the custom converter (by default it returns the same object)
     * @param value - upcoming kafka object
     * @return - converted value (if converter is present)
     * @throws InstantiationException - class instance ex
     * @throws IllegalAccessException - class instance ex
     */
    private Object convertValue(Object value) throws InstantiationException, IllegalAccessException {
        ExternalTable tableAn = value.getClass().getAnnotation(ExternalTable.class);
        return tableAn.converter().newInstance().convert(value);
    }

    /**
     * getting Table name of entity equals by entityName
     * @return - list of tables
     */
    private List<String> getTopicsNames() {
        return scanUtil.findClasses(ExternalTable.class).stream()
                .map(aClass -> aClass.getAnnotation(Table.class).name())
                .collect(Collectors.toList());
    }
}
