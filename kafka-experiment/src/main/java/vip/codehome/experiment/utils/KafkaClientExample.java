package vip.codehome.experiment.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaClientExample {

    public void sendAysnc(){
        Map<String,Object> confis=new HashMap<>();
        confis.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
        confis.put(ProducerConfig.CLIENT_ID_CONFIG,"k2");
        confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.ACKS_CONFIG,"0");
        confis.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000);
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer(confis);
    }
    public void recvAutoCommit(String topicName){
        Map<String,Object> confis=new HashMap<>();
        confis.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
        confis.put(ProducerConfig.CLIENT_ID_CONFIG,"k2");
        confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.ACKS_CONFIG,"0");
        confis.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000);
        KafkaProducer<byte[],byte[]> kafkaProducer=new KafkaProducer(confis);
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"zyw-record");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1000);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(topicName));
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> record:records){
                //System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset()+",data:"+record.value());
              JSONObject jsonObject= JSON.parseObject(record.value());
                ProducerRecord<byte[],byte[]> producerRecord=new ProducerRecord<>("TEST_KAFKA_PARTITION",jsonObject.getString("orderID").toString().getBytes(StandardCharsets.UTF_8),record.value().getBytes(StandardCharsets.UTF_8));
                kafkaProducer.send(producerRecord);
            }
        }
    }
    public void sendPartition(){
        Map<String,Object> confis=new HashMap<>();
        confis.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
        confis.put(ProducerConfig.CLIENT_ID_CONFIG,"k2");
        confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.ACKS_CONFIG,"0");
        confis.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000);
        KafkaProducer<byte[],byte[]> kafkaProducer=new KafkaProducer(confis);
        ProducerRecord<byte[],byte[]> p1=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",0,null,"11".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p2=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",0,null,"21".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p3=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",0,null,"31".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p4=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",1,null,"41".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p5=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",1,null,"51".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p6=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",1,null,"61".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p7=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",2,null,"71".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p8=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",2,null,"81".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[],byte[]> p9=new ProducerRecord<>("RECV_UAV_TRACK_FROM_UOM_V2",2,null,"91".getBytes(StandardCharsets.UTF_8));
        kafkaProducer.send(p1);
        kafkaProducer.send(p2);
        kafkaProducer.send(p3);
        kafkaProducer.send(p4);
        kafkaProducer.send(p5);
        kafkaProducer.send(p6);
        kafkaProducer.send(p7);
        kafkaProducer.send(p8);
        kafkaProducer.send(p9);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void testRecvAllPartition(){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"zyw-record4");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList("RECV_UAV_TRACK_FROM_UOM_V2"));
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1000));
            for(ConsumerRecord<String,String> record:records){
                System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset()+",data:"+record.value());
            }
        }
    }
    public static void main(String[] args) {
        KafkaClientExample kafkaClientExample=new KafkaClientExample();
   //     kafkaClientExample.recvAutoCommit("RECV_UAV_TRACK_FROM_UOM_V1");
      //  kafkaClientExample.sendPartition();
       kafkaClientExample.testRecvAllPartition();
    }
}
