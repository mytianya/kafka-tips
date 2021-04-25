package vip.codehome.experiment.rebalance;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

/**
 * @author zyw
 * @mail dsyslove@163.com
 * @createtime 2021/4/18--22:39
 * @description
 **/
public class KafkaRebalanceTest {
  Consumer consumer;
  @Before
  public void setConsumer(){
    Map<String,Object> configs=new HashMap<String,Object>();
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"192.168.31.95:9092");
    configs.put(CommonClientConfigs.GROUP_ID_CONFIG,"rebalance2");
    configs.put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG,1000*60);
    configs.put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG,1000*20);
    configs.put(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG,1000);
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,1);
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    consumer=new KafkaConsumer(configs);
  }
  @Test
  public void testConsumer() throws InterruptedException {
    consumer.subscribe(Arrays.asList("NOTE_LOGS"),new MyRebalanceListener());
    while (true){
      ConsumerRecords<byte[],byte[]> records=consumer.poll(Duration.ofSeconds(1));
      for(ConsumerRecord<byte[],byte[]> record:records){
        System.out.printf("offset:%d,value:%s,partition:%d\n",record.offset(),new String(record.value()),record.partition());
      }
      if(records.isEmpty()){
        TimeUnit.SECONDS.sleep(5);
      }
    }
  }
}
