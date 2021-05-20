package vip.codehome.experiment.utils;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/***
 * @author 道士吟诗
 * @date 2021/4/25-下午10:50
 * @description
 ***/
public class KafkaReciver {
    /**
     * 自动提交位移
     * @param topicName
     */
    public void recvAutoCommit(String topicName){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"c11");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(topicName));
        while (true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,String> record:records){
                System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset());
            }
        }
    }

    /**
     * 手动提交位移
     * @param topicName
     */
    public void recvManualCommit(String topicName){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"c0");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(topicName));
        try{
            while (true){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record:records){
                    System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset());
                }
                //异步提交
                consumer.commitAsync();
            }
        }finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    /**
     * 手动提交位移
     * @param topicName
     */
    public void recvListener(String topicName){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"c0");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //在开启新一轮重平衡之前的操作
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
                consumer.commitSync(toCommit);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //在重平衡分配好之后的操作
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap=consumer.committed(new HashSet<>(partitions));
                for(TopicPartition topicPartition:offsetAndMetadataMap.keySet()){
                    consumer.seek(topicPartition,offsetAndMetadataMap.get(topicPartition).offset());
                }
            }
        });
        try{
            while (true){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record:records){
                    System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset());
                }
                //异步提交
                consumer.commitAsync();
            }
        }finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    /**
     * 指定分区消费
     */
    public void recvPartition(String topicName){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"c20");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        TopicPartition partition0=new TopicPartition(topicName,0);
        consumer.assign(Arrays.asList(partition0));
        try{
            while (true){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record:records){
                    System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset());
                }
                //异步提交
                consumer.commitAsync();
            }
        }finally {
            consumer.commitSync();
            consumer.close();
        }
    }
    /**
     * 指定消费offset位置
     */
    public void seek(String topicName){
        Map<String,Object> configs=new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"c1");
        //是否开启自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(topicName));
        Set<TopicPartition> assignment=new HashSet<>();
        while (assignment.size()==0){
            consumer.poll(100);
            //获取消费者分配的分区
            assignment=consumer.assignment();
        }
        for(TopicPartition topicPartition:assignment){
            System.out.println("当前获取话题到的分区:"+topicPartition.topic()+",partition:"+topicPartition.partition());
            //从头开始消费
            consumer.seek(topicPartition,300);
        }
        try{
            while (true){
                ConsumerRecords<String,String> records=consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String,String> record:records){
                    System.out.println("topic:"+record.topic()+",partition:"+record.partition()+",offset:"+record.offset()+",data:"+record.value());
                }
                //异步提交
                consumer.commitAsync();
            }
        }finally {
            consumer.commitSync();
            consumer.close();
        }
    }

}
