package com.test;

import java.util.Collections;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**args 第一个入参是topic；第二个是消费方式
 * @author Jianping Yu
 * @date 2021/4/23
 */
@Slf4j
public class ConsumerObu {
    public static void main(String[] args) throws Exception {

        System.out.println("start");
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-0.icos.city:29094,kafka-1.icos.city:29094,kafka-2.icos.city:29094");
        props.put("group.id", "obu1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 使用Confluent实现的KafkaAvroDeserializer
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 添加schema服务的地址，用于获取schema
//        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        props.put("schema.registry.url", "http://icosevent-schemaregistry-service-icos.icos.icos.city/");
        if(args.length>1) {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, args[1]);
        }else {
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

//        consumer.subscribe(Collections.singletonList("EVENT.datang.obubsm.telemetrytest"));
        consumer.subscribe(Collections.singletonList(args[0]));
        log.info("start------------------");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                log.info("records.size="+records.count());
                for (ConsumerRecord<String, String> record : records) {
                    Thread.sleep(10);
                    String user = record.value();
                    System.out.println(" value="+user);
                    log.info(" value="+user
                            + "\npartition = " + record.partition() + ", " + "offset = " + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
