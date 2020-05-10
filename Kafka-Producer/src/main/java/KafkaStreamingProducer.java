import com.oracle.tools.packager.Log;
import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.clients.producer.Callback;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import java.io.*;
import java.util.zip.GZIPInputStream;



public class KafkaStreamingProducer {

    Logger logger = LoggerFactory.getLogger(KafkaStreamingProducer.class.getName());

    public KafkaStreamingProducer(){}

    public static void main(String[] args) {

        new KafkaStreamingProducer().run();

    }

    public void run(){

        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        Streaming(msgQueue);

//      create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

//      add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.logger.info("stopping application...");
            this.logger.info("closing producer...");
            producer.close();
            this.logger.info("done!");
        }));

//        loop to send data to kafka
        // on a different thread, or multiple different threads....
        while (true) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
//                client.stop();
            }
            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord("consumer_reviews", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Something bad happened!", e);
                        }
                    }
                });
            }
        }
//        logger.info("End of application");

    }

    public void Streaming(BlockingQueue<String> msgQueue){


        try {

            String path1 = "/Users/utkarshnath/Desktop/reviewData.csv";

            BufferedReader br = new BufferedReader(new FileReader(path1));
            String head = br.readLine();
            String[] features = head.split(",");
            for(int i=0; i<features.length; i++) {
                logger.info(i+" "+features[i]);
            }
            for (int i = 0; i < 100; i++) {
                String line = br.readLine();
                String[] data = line.split(",");
                logger.info(data[0]+" ");
                JSONObject jsonString = new JSONObject();

                for (int k = 0; k < data.length; k++) {

                    logger.info(k+" "+ data[k]);

                    jsonString.put(features[k], data[k]);
                }
                msgQueue.add(String.valueOf(jsonString));
                System.out.println(jsonString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";

//        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create a safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

//        high throughput Producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

//        create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}

