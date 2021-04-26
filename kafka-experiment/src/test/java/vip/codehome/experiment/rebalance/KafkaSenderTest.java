package vip.codehome.experiment.rebalance;

import org.junit.Before;
import org.junit.Test;
import vip.codehome.experiment.utils.KafkaSender;

/***
 * @author 道士吟诗
 * @date 2021/4/25-下午9:37
 * @description
 * @href https://zhmin.github.io/2019/05/20/kafka-transaction/ kafka事务实现原理
 * @href https://www.cnblogs.com/wangzhuxing/p/10125437.html kafka事务解释
 ***/
public class KafkaSenderTest {
    KafkaSender kafkaSender;
    @Before
    public void setKafkaSender(){
        kafkaSender=new KafkaSender();
    }
    @Test
    public void testSend() throws InterruptedException {
        for(int i=0;i<100;i++){
            kafkaSender.sendAsync("TEST","","hello kafka");
        }
        //kafkaSender.sendSync("TEST","","hello kafka");
        Thread.sleep(1000);
    }
    @Test
    public void testSendTx() throws InterruptedException {
        //kafkaSender.sendSync("TEST","","hello kafka");
        kafkaSender.sendTx("TEST","","hello kafka tx");
        Thread.sleep(1000);
    }
}
