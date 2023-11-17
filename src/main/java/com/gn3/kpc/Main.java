package com.gn3.kpc;

import com.gn3.kpc.dto.DTO;
import com.gn3.kpc.dto.NewsDTO;
import com.gn3.kpc.service.ChatGPTService;
import com.gn3.kpc.service.ChatGPTServiceImpl;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;

import java.io.IOException;
import java.time.Duration;
import java.util.*;


public class Main {
    private static final CharSequence FIN_MESSAGE = "end";
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        AnnotationConfigApplicationContext ac = new AnnotationConfigApplicationContext(AutoConfig.class);

        String bootstrapServers = "master:9092,sn01:9092,sn02:9092,sn03:9092";
        String groupId = args[0];
        String topic = "news";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        List<DTO> newsDTOS = new ArrayList<>();
        try {
            consumer.subscribe(Arrays.asList(topic));
            String message = "";
            break_point: while (true){
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    message = record.value();
                    if(message.equals(FIN_MESSAGE))break break_point;
                    try{
                        Gson gson = new GsonBuilder().create();
                        NewsDTO newsDTO = gson.fromJson(message, NewsDTO.class);
                        newsDTOS.add(newsDTO);
                    }catch (Exception e){
                        e.printStackTrace();
                        System.out.println("message = " + message);
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            log.info("The consumer is now gracefully closed.");
        }

        JavaSparkContext javaSparkContext = ac.getBean(JavaSparkContext.class);
        JavaRDD<DTO> rdd = javaSparkContext.parallelize(newsDTOS, 2);
        JavaRDD<String> emotionResult = rdd.mapPartitions(iter -> {
            List<DTO> dtos = new ArrayList<>();
            iter.forEachRemaining(dtos::add);
            ChatGPTService chatGPTService = new ChatGPTServiceImpl();
            List<String> result = new ArrayList<>();
            for (DTO dto : dtos) {
                String content = ((NewsDTO) dto).getContent();
                String gptResult = chatGPTService.chatGPT(content, "\n 이 기사의 감성을 긍정 혹은 부정으로 분석해줘");
                if (gptResult.contains("긍정")) result.add("긍정");
                else if (gptResult.contains("부정")) result.add("부정");
            }

            return result.iterator();
        });
        List<Tuple2<String, Integer>> result = emotionResult.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2).collect();
        Jedis jedis = ac.getBean(JedisPool.class).getResource();
        for (Tuple2<String, Integer> tuple : result) {
            jedis.set(tuple._1(), String.valueOf(tuple._2()));
        }
        jedis.close();
    }
}
