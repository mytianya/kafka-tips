package vip.codehome.experiment.utils;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author dsys
 * @version v1.0
 **/
public class RecvCountInterceptor implements ConsumerInterceptor {

  @Override
  public ConsumerRecords onConsume(ConsumerRecords records) {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public void onCommit(Map offsets) {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
