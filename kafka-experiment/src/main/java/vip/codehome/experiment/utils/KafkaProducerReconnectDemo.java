package vip.codehome.experiment.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author dsys
 * @version v1.0
 **/
public class KafkaProducerReconnectDemo {
  public static final String text =
      "ZCZC KSJ2945 091600\n"
          + "GG ZSSHYMYX\n"
          + "091600 ZSSSYZYX\n"
          + "SACI34 ZSNJ 091600\n"
          + "METAR ZSNJ 091600Z 11001MPS CAVOK 22/19 Q1005 NOSIG=\n"
          + "\n"
          + "NNNN\n";

  public static void main(String[] args) {
    Map<String,Object> confis=new HashMap<>();
    confis.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.28.4.23:9092,192.28.4.24:9092,192.28.4.25:9092");
    //   confis.put(ProducerConfig.CLIENT_ID_CONFIG,"k2");
    confis.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    confis.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    confis.put(ProducerConfig.ACKS_CONFIG,"all");
    confis.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000);
    confis.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("vip.codehome.experiment.utils.SendCountInterceptor"));
    KafkaProducer kafkaProducer=new KafkaProducer(confis);
    for(int i=0;i<200;i++){
      ProducerRecord<String,String> record= new ProducerRecord("REP1",String.valueOf(i),text);
      Future<RecordMetadata> future=kafkaProducer.send(record);
      try {
        RecordMetadata recordMetadata=future.get();
        System.out.println("当前值："+String.valueOf(i)+"current partition:"+recordMetadata.partition()+",current offset:"+recordMetadata.offset());
      } catch (Exception e){
        e.printStackTrace();
      }
    }
  kafkaProducer.close();
  }
}
