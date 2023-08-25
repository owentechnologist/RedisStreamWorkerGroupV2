package com.redislabs.sa.ot.rswgv2;
import com.github.javafaker.Faker;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.XAddParams;

import java.util.HashMap;
import java.util.Map;

public class StreamWriter {

    private Pipeline jedisPipeline;
    private long sleepTime = 50l;//milliseconds
    private long batchSize = 200;
    private long totalNumberToWrite = 1000;
    private String streamName,payloadKeyName;
    private static Faker faker = new Faker();

    public StreamWriter(String streamName, Pipeline jedisPipeline){
        this.jedisPipeline=jedisPipeline;
        this.streamName=streamName;
    }

    public StreamWriter setTotalNumberToWrite(long totalNumberToWrite){
        this.totalNumberToWrite=totalNumberToWrite;
        return this;
    }
    public StreamWriter setBatchSize(long batchSize){
        this.batchSize=batchSize;
        return this;
    }
    public StreamWriter setSleepTime(long sleepTime){
        this.sleepTime=sleepTime;
        return this;
    }
    public StreamWriter setPayloadKeyName(String payloadKeyName){
        this.payloadKeyName=payloadKeyName;
        return this;
    }

    public void kickOffStreamEvents(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, String> map1 = new HashMap<>();
                long totalWrittenCounter = 1;
                while (true) {
                    if(totalNumberToWrite-totalWrittenCounter<=batchSize) {
                        batchSize=(totalNumberToWrite-totalWrittenCounter);
                    }
                    for (int batchCounter = 0; batchCounter < batchSize; batchCounter++) {
                        String payload = faker.name().firstName()+" "+faker.name().lastName()+" "+faker.address().secondaryAddress();
                        map1.put(payloadKeyName, payload);
                        jedisPipeline.xadd(streamName, XAddParams.xAddParams(), map1);
                    }
                    jedisPipeline.sync();
                    totalWrittenCounter=totalWrittenCounter+batchSize;
                    if(totalWrittenCounter%1000 == 0){
                        //check for new StreamName (old one is getting old)
                        long ttl = 0;
                        Response<Long> secondsLeftForStream  = jedisPipeline.ttl(streamName);
                        jedisPipeline.sync();
                        ttl = secondsLeftForStream.get().longValue();
                        if(ttl<60){
                            //need to create a new key and start writing to it instead of the old one
                            System.out.println("[StreamWriter] asking for new Active StreamKey --> TTL on "+streamName+" : --> "+ttl);
                            streamName = StreamLifecycleManager.makeSecondaryStream();
                        }
                    }
                    try{
                        Thread.sleep(sleepTime);
                    }catch(InterruptedException ie){}

                    if(totalWrittenCounter>=totalNumberToWrite){
                        break;
                    }
                }
            }
        }).start();
    }
}
