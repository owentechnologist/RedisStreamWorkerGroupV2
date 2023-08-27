package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class inrements a counter to track progress for every processed Stream Entry
 * The counter name will be equal to the defined Response StreamName
 */
public class StreamEventMapProcessorCounterIncr implements StreamEventMapProcessor {

    //make sure to set this value before passing this processor to the Stream Adapter
    private Object callbackTarget = null;
    private JedisPooled jedisPooled = null;
    static AtomicLong counter = new AtomicLong();
    private Long sleepTime = null;//millis
    private String outputStreamName = null;
    private String payloadKeyName = "stringOffered";
    private boolean verbose = false;
    private JedisConnectionHelper jedisConnectionHelper = null;

    //chain these methods to configure various properties
    public StreamEventMapProcessorCounterIncr setJedisConnectionHelper(JedisConnectionHelper jedisConnectionHelper) {
        this.jedisConnectionHelper = jedisConnectionHelper;
        jedisPooled = jedisConnectionHelper.getPooledJedis();
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorCounterIncr setPayloadKeyName(String payloadKeyName) {
        this.payloadKeyName = payloadKeyName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorCounterIncr setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorCounterIncr setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorCounterIncr setSleepTime(long sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        printMessageSparingly("\nStreamEventMapProcessorToStream.processStreamEventMap()>>\t" + payload.keySet());
        doSleep(sleepTime); // the point of this is to simulate slower workers
        for( String se : payload.keySet()) {
            printMessageSparingly(payload.get(se).toString());
            StreamEntry x = payload.get(se);
            Map<String,String> m = x.getFields();
            String aString = "";
            for( String f : m.keySet()){
                printMessageSparingly("key\t"+f+"\tvalue\t"+m.get(f));
                if(f.equalsIgnoreCase(payloadKeyName)){
                    String originalString = m.get(f);
                    if(originalString.equalsIgnoreCase("poisonpill")){
                        System.out.println("\n\n\t\tSIMULATING FAILED STATE...");
                        doSleep(90000l);
                        throw new NullPointerException("DELIBERATELY KILLING MYSELF ON POISONPILL");
                    }
                    String calcValue = doCalc(originalString);
                    String originalId = se.split(" ")[0];
                    updateCounter();
                    //writeToRedisStream(originalId,originalString,calcValue);
                }
            }
        }
    }

    void printMessageSparingly(String message){
        int skipSize = 1000;
        if((counter.get()%skipSize==0)&&(verbose)) {
            System.out.println("This second message printed 1 time for each "+skipSize+" events:\n"+message);
        }
    }

    void updateCounter(){
        try {
            //using same name as was assigned to outputStreamName for convenience, but it is a String
            // we increment the string as a counter to show work is progressing for this consumer:
            jedisConnectionHelper.getPooledJedis().incr(outputStreamName);
        } catch (Throwable t) {
            System.out.println("WARNING:");
            t.printStackTrace();
        }
    }

    // goofy example reverses the received string argument client-side and submits it reversed as result
    String doCalc(String aString){
        StringBuffer reversedString = new StringBuffer(aString).reverse();
        return reversedString.toString();
    }

    void doSleep(long sleepTime){
        try{
            Thread.sleep(sleepTime);
        }catch(InterruptedException ie){}
    }

    public void setCallbackTarget(Object callbackTarget){
        this.callbackTarget = callbackTarget;
    }
}
