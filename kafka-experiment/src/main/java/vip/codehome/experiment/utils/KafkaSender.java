package vip.codehome.experiment.utils;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/***
 * @author 道士吟诗
 * @date 2021/4/25-下午9:15
 * @description
 * 1. 同步发送
 * 2. 异步发送
 * 3. 幂等发布
 * 4. 事务消息
 ***/
public class KafkaSender {

    public KafkaProducer getKafkaProducer(){
        Map<String,Object> confis=new HashMap<>();
        confis.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.7.25:9092");
     //   confis.put(ProducerConfig.CLIENT_ID_CONFIG,"k2");
   //     confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      //  confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        confis.put(ProducerConfig.ACKS_CONFIG,"all");
        confis.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000);
        //发送重试

        //事务id
    //    confis.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"tid-1");
        //开启幂等性
     //   confis.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        return  new KafkaProducer(confis);
    }

    /**
     * 同步发送
     * @param key
     * @param value
     */
    public void sendSync(String topicName,String key,String value){
        ProducerRecord<String,String> record= new ProducerRecord(topicName,key,value);
        Future<RecordMetadata> future=getKafkaProducer().send(record);
        try {
            RecordMetadata recordMetadata=future.get();
            System.out.println("current partition:"+recordMetadata.partition()+",current offset:"+recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送
     * @param topicName
     * @param key
     * @param value
     */
    public void sendAsync(String topicName,String key,String value){
        ProducerRecord<byte[],byte[]> record= new ProducerRecord(topicName,key.getBytes(StandardCharsets.UTF_8),value.getBytes(StandardCharsets.UTF_8));
        getKafkaProducer().send(record, new Callback() {
           @Override
           public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
               if(exception==null){
                   System.out.println("current partition:"+recordMetadata.partition()+",current offset:"+recordMetadata.offset());
               }else{
                   exception.printStackTrace();
               }
           }
       });

    }
    public void sendTx(String topicName,String key,String value){
        KafkaProducer<String,String> kafkaProducer=getKafkaProducer();
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,key,value);
        Future<RecordMetadata> future=kafkaProducer.send(record);
        try {
            RecordMetadata recordMetadata=future.get();
            System.out.println("current partition:"+recordMetadata.partition()+",current offset:"+recordMetadata.offset());
            kafkaProducer.flush();
            kafkaProducer.commitTransaction();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
