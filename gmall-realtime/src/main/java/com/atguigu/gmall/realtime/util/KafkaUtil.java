package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author GraceBreeze
 * @create 2022-08-22 15:09
 */
public class KafkaUtil {
    private static String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topicName, String groupID) {

        //创建配置对象
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(topicName, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord == null || consumerRecord.value() == null) {
                    return "";
                }
                return new String(consumerRecord.value());
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return BasicTypeInfo.STRING_TYPE_INFO;
            }
        }
                , properties);
        return flinkKafkaConsumer;
    }


    public static FlinkKafkaProducer<String> getKafkaProducer(String topicName) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);

        return new FlinkKafkaProducer<String>(topicName, new SimpleStringSchema(),properties);
    }


//    public static String getKafkaDDL(String topicName, String groupID) {
//        return "with "
//    }



}




