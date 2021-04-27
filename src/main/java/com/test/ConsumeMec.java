package com.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * @author Jianping Yu
 * @date 2021/4/27
 */
@Slf4j
public class ConsumeMec {
    public static void main(String[] args) throws InterruptedException, IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter("mecData.txt"));
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-0.icos.city:29094,kafka-1.icos.city:29094,kafka-2.icos.city:29094");
        props.put("group.id", "mec-yjp");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 使用Confluent实现的KafkaAvroDeserializer
//        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 添加schema服务的地址，用于获取schema
//        props.put("schema.registry.url", "http://icosevent-schemaregistry-service-icos.icos.icos.city/");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        log.info("start==================");

        consumer.subscribe(Collections.singletonList("mec-v1-v2xpublish"));//Mec的topic
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                log.info("records.size="+records.count());
                for (ConsumerRecord<String, String> record : records) {
                    Thread.sleep(10);
                    String user = record.value();
                    log.info(" MEcValue="+user
                            + "\npartition = " + record.partition() + ", " + "offset = " + record.offset());
                    bf.write(record.value()+"\n");
                }
            }
        } finally {
            consumer.close();
            bf.close();
        }
    }
}
