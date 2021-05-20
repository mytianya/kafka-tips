package vip.codehome.experiment.utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRebalanceDemo {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static KafkaConsumer createKafkaConsumer(String groupId) {
		HashMap<String, Object> propsMap = new HashMap<>();
		// 服务器地址
		propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.28.4.23:9092,192.28.4.24:9092,192.28.4.25:9092");
		// 是否开启自动提交位移
		propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		// 自动提交位移的时间
		propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
		// 最长没收到心跳时间6s
		propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000 * 6);
		// 心跳间隔2s,至少能发送3此心跳
		propsMap.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000 * 2);
		propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
		propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
		// 消费方式
		propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		propsMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
				// 最大消息大小
				10485760);
		propsMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760);
		propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		// 两次提交消息位移
	//	propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
	propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
		// 一次拉取消息最大个数
		propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
		return new KafkaConsumer(propsMap);
	}

	public void recv() {
		KafkaConsumer consumer = createKafkaConsumer("rebalance-test-3");
		consumer.subscribe(Arrays.asList("REBALANCE_TOPIC_TEST"));
		try {
			while (true) {
				ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
				for (ConsumerRecord<byte[], byte[]> record : records) {
					logger.debug("接收到消息{},offset:{},partition:{}}", record.value(), record.offset(),
							record.partition());
					byte[] msg = record.value();
					try {
						  logger.info("接收到消息{},offset:{},partition:{}}", record.value(), record.offset(),
					              record.partition());
						  System.out.println("hahahahahahaha........");
					} catch (Exception e) {
						logger.error("消息{},offset:{},partition:{},处理失败{}", record.value(), record.offset(),
								record.partition(), e);
					} finally {
						consumer.commitAsync((Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) -> {
							if (exception != null) {
								StringBuffer sb = new StringBuffer();
								for (TopicPartition topicPartition : offsets.keySet()) {
									sb.append(topicPartition.topic() + "," + topicPartition.partition() + ","
											+ offsets.get(topicPartition).offset() + "\t");
								}
								logger.error("异步提交位移失败:" + sb.toString(), exception);
							}
						});// // 使用异步提交规避阻塞
					}
				}
			}
		} finally {
			try {
				consumer.commitSync();// 最后一次提交使用同步阻塞提交，确保位移提交成功
			} finally {
				consumer.close();
			}
		}
	}
	public static void main(String[] args) {
		KafkaRebalanceDemo demo=new KafkaRebalanceDemo();
		demo.recv();
	}
}
