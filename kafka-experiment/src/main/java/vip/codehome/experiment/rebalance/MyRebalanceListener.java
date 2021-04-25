package vip.codehome.experiment.rebalance;

import java.util.Arrays;
import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * @author zyw
 * @mail dsyslove@163.com
 * @createtime 2021/4/18--23:23
 * @description
 **/
public class MyRebalanceListener implements ConsumerRebalanceListener {

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    System.out.println("onPartitionsRevoked:"+ Arrays.toString(partitions.toArray()));
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    System.out.println("onPartitionsAssigned:"+ Arrays.toString(partitions.toArray()));

  }
}
