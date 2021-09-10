package vip.codehome.experiment.utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dsys
 * @version v1.0
 **/
public class SendCountInterceptor implements ProducerInterceptor {
  private Logger logger= LoggerFactory.getLogger(SendCountInterceptor.class);
  private AtomicInteger total=new AtomicInteger(0);
  private AtomicInteger errorCounter=new AtomicInteger(0);
  private AtomicInteger successCounter=new AtomicInteger(0);
  @Override
  public ProducerRecord onSend(ProducerRecord record) {
    total.incrementAndGet();
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    if(exception==null){
      successCounter.incrementAndGet();
    }else{
      errorCounter.incrementAndGet();
    }
  }

  @Override
  public void close() {
    logger.info("本次发送成功条数:{},本次发送失败条数:{}",successCounter.get(),errorCounter.get());
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
