package pku;

import java.io.File;
import java.io.FileOutputStream;

/**
 * 生产者
 */
public class Producer {

	//生成一个指定topic的message返回
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    public ByteMessage createBytesMessageToTopic(int topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    public ByteMessage createBytesMessageToTopic(long topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    public ByteMessage createBytesMessageToTopic(double topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    //将message发送出去
    public void send(ByteMessage defaultMessage){
        DemoMessageStore.store.push(defaultMessage);
    }
    //处理将缓存区的剩余部分
    public void flush()throws Exception{
//        DemoMessageStore.bufferedOutputStream.flush();
    }
}
