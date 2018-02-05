import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerDemo implements Runnable{
    private final KafkaConsumer<String,String> consumer ;
    private final List<String> topics ;
    private final int id ;
    public ConsumerDemo(int id,String groupId,List<String> topics){
        this.id=id;
        this.topics=topics;
        Properties prop=new Properties();
        prop.put("bootstrap.servers", "10.0.2.17:9092,10.0.2.18:9092");
        prop.put("group.id", groupId);

        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        prop.put("enable.auto.commit","false");
        prop.put("session.timeout.ms", "30000");
        prop.put("max.poll.records", 10000);
        //大概是控制单条数据过大的问题 先调300 试试
        prop.put("auto.commit.interval.ms","25000");

        prop.put("max.partition.fetch.bytes","1000000");

        prop.put("request.timeout.ms",50000);


        this.consumer = new KafkaConsumer<String, String>(prop);
    }

    public void shutdown() {
        consumer.wakeup();
    }
    private static int i=0;

    public void run(){
        try {
            consumer.subscribe(topics);

            while (true){
                ConsumerRecords<String,String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String,String> record:records) {

                    System.out.println(record);
                    System.out.println(i);
                    i++;
                }
            }
        }catch (Exception e){

        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        int numConsumer=3;
        String groupId ="group1";
        List<String> topics = Arrays.asList("lmx_text12");

        ExecutorService execuotr = Executors.newFixedThreadPool(numConsumer);

        final List<ConsumerDemo> consumers = new ArrayList<>();
        for (int i=0;i<numConsumer;i++){
            ConsumerDemo consumer = new ConsumerDemo(i,groupId,topics);
            consumers.add(consumer);
            execuotr.submit(consumer);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(){

            @Override
            public void run() {
                for (ConsumerDemo consumer:consumers) {
                    consumer.shutdown();
                }
                execuotr.shutdown();
                try {
                    execuotr.awaitTermination(5000, TimeUnit.MICROSECONDS);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

    }




    /*
     .put("bootstrap.servers", servers)
                .put("group.id", group)
                //自动提交 or手动提交
                .put("enable.auto.commit", "false")
                //consumer session 过期时间
                .put("session.timeout.ms", "30000")
                //key  序列化类型
                .put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                //value 反序列化类型
                .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                //抽取最大max数量
                .put("max.poll.records", 10000)
                //大概是控制单条数据过大的问题 先调300 试试
                .put("auto.commit.interval.ms","10000")
     */
//    public static void main(String[] args) {
//        int i=1;
//        Properties prop=new Properties();
//        prop.put("bootstrap.servers", "10.0.2.17:9092,10.0.2.18:9092");
//        prop.put("group.id", "text-grop1");
//
//        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        prop.put("enable.auto.commit","false");
//        prop.put("session.timeout.ms", "30000");
//        prop.put("max.poll.records", 10000);
//
//        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
//        consumer.subscribe(Arrays.asList("lmx_text10"));
//
//        try {
//            while (true){
//                ConsumerRecords<String,String> records=consumer.poll(1000);
//                for (ConsumerRecord<String,String> record:records
//                     ) {
//                    System.out.println(record);
//                    i++;
//                    System.out.println("num:  "+i);
//
//                }
//            }
//        }finally {
//            consumer.close();
//
//        }
//
//    }
}
