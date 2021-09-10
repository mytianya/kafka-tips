package vip.codehome.experiment.utils;

import java.awt.RadialGradientPaint;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author dsys
 * @version v1.0
 **/
public class KafkaConsumerReconnectDemo {

  public static void main(String[] args) {
    Random random=new Random(1000);
    Map<String,Object> configs=new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.4.23:9092,192.28.4.24:9092,192.28.4.25:9092");
    configs.put(ConsumerConfig.GROUP_ID_CONFIG,"zyw");
    //是否开启自动提交
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,6000);
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1);
    KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
    consumer.subscribe(Arrays.asList("REP1"));
    try{
      while (true){
        ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
        for(ConsumerRecord<String,String> record:records){
          try {
            System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset()+",value:"+record.value());
          } catch (Exception e) {
            e.printStackTrace();
          }finally{
//            consumer.commitAsync((Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception)->{
//              if(exception!=null){
//                StringBuffer sb=new StringBuffer();
//                for(TopicPartition topicPartition:offsets.keySet()){
//                  sb.append(topicPartition.topic()+","+topicPartition.partition()+","+offsets.get(topicPartition).offset()+"\t");
//                }
//               System.out.println("异步提交位移失败:"+sb.toString()+exception);
//              }
//            });
            try{
              consumer.commitSync();
            }catch (Exception e){
              e.printStackTrace();
            }
          }
        }
        }
    }finally {
          try {
            consumer.commitSync();//最后一次提交使用同步阻塞提交，确保位移提交成功
          } finally {
            consumer.close();
          }
    }
  }
}
