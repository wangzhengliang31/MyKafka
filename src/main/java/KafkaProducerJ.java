import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerJ {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");   // acks指定必须要有多少个分区副本收到消息生产者才会认为写入成功并收到服务器响应，0：不等待，1：leader收到即可，all：所有副本节点都收到
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.wzl.KafkaProducerPartitionerJ");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i));

            // send返回一个包含RecordMetadata的Future对象
            producer.send(record);  // 发送并忘记
//            producer.send(record).get()// 同步发送消息，调用Future对象的get方法等待kafka响应，如果服务器返回错误，get会抛出异常
//            producer.send(record, new ProducerCallback());  // 异步发送消息，服务器返回响应时调用回调函数
        }
        producer.close();

    }

    // 异步回调函数
    public class ProducerCallback implements Callback {

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e != null){
                e.printStackTrace();
            }
        }
    }
}
