package com.redislabs.sa.ot.rswgv2;
import com.github.javafaker.Faker;
import redis.clients.jedis.Pipeline;
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
                        String payload = faker.name().firstName();
                        map1.put(payloadKeyName, payload);
                        jedisPipeline.xadd(streamName, XAddParams.xAddParams(), map1);
                    }
                    jedisPipeline.sync();
                    totalWrittenCounter=totalWrittenCounter+batchSize;
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
