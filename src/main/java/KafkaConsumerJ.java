import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerJ {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");   // acks指定必须要有多少个分区副本收到消息生产者才会认为写入成功并收到服务器响应，0：不等待，1：leader收到即可，all：所有副本节点都收到
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("group.id", "test01");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("CountriesTest"));

//        // 再均衡监听器
//        consumer.subscribe(Collections.singletonList("CountriesTest"), new HandleRebalance());

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

//                    // 提交特定的偏移量
//                    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
//                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, null));
//                    if(record.offset() % 1000 == 0){
//                        consumer.commitAsync(currentOffsets, null);
//                    }

//                    // 存储offset到数据库
//                    commitOffsetToDB(record.topic(), record.partition(), record.offset());
                }

//                // offset从数据库中取出，consumer从这里开始消费
//                for(TopicPartition partition : consumer.assignment()){
//                    consumer.seek(partition, getOffsetFromDB(partition));
//                }

                try {
                    // 同步批量完成后提交offset
                    consumer.commitSync();

//                    // 异步批量完成后提交offset
//                    consumer.commitAsync();

//                    // 异步批量完成后提交offset后回调
//                    consumer.commitAsync(new OffsetCommitCallback() {
//                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                            if(exception != null) {
//                                exception.printStackTrace();
//                            }
//                        }
//                    });

//                   // 同步异步组合提交，最后一次同步提交
//                    try {
//                        // TODO ......
//                        consumer.commitAsync();
//                    } finally {
//                        try {
//                            consumer.commitSync();
//                        } finally {
//                            consumer.close();
//                        }
//                    }



                } catch (CommitFailedException e) {
                    e.printStackTrace();
                }

            }
        } catch (WakeupException e){        // 消费者退出循环
            // 另一个线程调用wakeup，导致poll抛出WakeupException
        }
        finally {
            consumer.close();
        }

    }

    // 再均衡监听器，消费者在退出和进行分区再均衡之前做一些清理工作
    public class HandleRebalance implements ConsumerRebalanceListener {

        // 再均衡开始之前和消费者停止读取消息之后被调用
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//            consumer.commitSync(currentOffsets);

//            // offset事务保存到数据库
//            commitOffsetDBTranscation();
        }

        // 重新分配分区之后和消费者开始读取消息之前使用
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

//            // offset从数据库中取出，consumer从这里开始消费
//            for(TopicPartition partition:partitions){
//                consumer.seek(partition, getOffsetFromDB(partition));
//            }
        }
    }

//    // 消费者退出循环,另一个线程调用wakeup，导致poll抛出WakeupException
//    Runtime.getRuntime().addShutDownHook(new Thread()){
//        public void run(){
//          consumer.wakeup();
//          try {
//              mainThread.join();
//          }catch (InterruptedException e){
//              e.printStackTrace();
//          }
//        }
//    });
}




