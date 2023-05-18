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
 * This class processes Event Entries passed to it by the RedisStreamWorkerHelper
 * It writes Events to a Redis stream that are evidence of the
 * work being completed
 * Some other process needs to clean up the resulting output stream
 */

public class StreamEventMapProcessorToStream implements StreamEventMapProcessor {

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
    public StreamEventMapProcessorToStream setJedisConnectionHelper(JedisConnectionHelper jedisConnectionHelper) {
        this.jedisConnectionHelper = jedisConnectionHelper;
        jedisPooled = jedisConnectionHelper.getPooledJedis();
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorToStream setPayloadKeyName(String payloadKeyName) {
        this.payloadKeyName = payloadKeyName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorToStream setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorToStream setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamEventMapProcessorToStream setSleepTime(long sleepTime)
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
                    writeToRedisStream(originalId,originalString,calcValue);
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

    void writeToRedisStream(String originalId,String originalString,String calcString){
        try {
            HashMap<String, String> map = new HashMap<>();
            map.put("arg_provided", originalString);
            map.put("calc_result", calcString);
            map.put(Runtime.getRuntime().toString()+"_Counter",""+counter.incrementAndGet());
            map.put("EntryProvenanceMetaData",originalId);
            jedisConnectionHelper.getPooledJedis().xadd(outputStreamName, StreamEntryID.NEW_ENTRY,map);
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