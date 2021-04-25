package vip.codehome.experiment.rebalance;

import org.junit.Before;
import org.junit.Test;
import vip.codehome.experiment.utils.KafkaReciver;

/***
 * @author 道士吟诗
 * @date 2021/4/25-下午11:11
 * @description
 ***/
public class KafkaReciverTest {
    KafkaReciver kafkaReciver;
    @Before
    public void setKafkaReciver(){
        kafkaReciver=new KafkaReciver();
    }
    @Test
    public void testSeek(){
        kafkaReciver.seek("TEST");
    }
    @Test
    public void testRecvPartition(){
        kafkaReciver.recvPartition("TEST");
    }
}
