package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XTrimParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingSummary;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StreamReaper implements StreamEventMapProcessor {

    //make sure to set this value before passing this processor to the Stream Adapter
    private Object callbackTarget = null;
    private JedisPooled jedisPooled = null;
    private int oneDay = 60 * 60 * 24 * 1000;
    static AtomicLong counter = new AtomicLong();
    private Long sleepTime = null;//millis
    private String outputStreamName = null;
    private String forProcessingStreamName = null;
    private String payloadKeyName = "stringOffered";
    private boolean verbose = false;
    private String consumerGroupName = "";
    private long printcounter = 0;
    private boolean shouldTrim;

    //chain these methods to configure various properties
    public StreamReaper setJedisConnectionHelper(JedisConnectionHelper jedisConnectionHelper) {
        this.jedisPooled = jedisConnectionHelper.getPooledJedis();
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setPayloadKeyName(String payloadKeyName) {
        this.payloadKeyName = payloadKeyName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setForProcessingStreamKeyName(String forProcessingStreamName) {
        this.forProcessingStreamName = forProcessingStreamName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    //chain these methods to configure various properties
    //outputStreamName might be 'X:triageStream' for this Reaper execution
    public StreamReaper setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setSleepTime(long sleepTime)
    {
        this.sleepTime = sleepTime;
        return this;
    }

    //chain these methods to configure various properties
    public StreamReaper setShouldTrimOutPutStream(boolean shouldTrim)
    {
        this.shouldTrim = shouldTrim;
        return this;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        printMessageSparingly("\nStreamReaper.processStreamEventMap()>>\t" + payload.keySet());
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
                        System.out.println("\n\n\t\tFIXING FAILED STATE...");
                        originalString = "Fix me please";
                    }
                    String calcValue = doCalc(originalString);
                    String originalId = se.split(" ")[0];
                    writeToRedisStream(originalId,originalString,calcValue);
                }
            }
        }
    }

    void printMessageSparingly(String message){
        int skipSize = 10;
        if((counter.get()%skipSize==0)&&(verbose)) {
            System.out.println("This reaper message printed 1 time for each "+skipSize+" events:\n"+message);
        }
    }

    void writeToRedisStream(String originalId,String originalString,String calcString){
        try {
            HashMap<String, String> map = new HashMap<>();
            map.put("special_status","MESSAGE RECOVERED AND PROCESSED BY REAPER");
            map.put("arg_provided", originalString);
            map.put("calc_result", calcString);
            map.put(Runtime.getRuntime().toString()+"_Counter",""+counter.incrementAndGet());
            map.put("EntryProvenanceMetaData",originalId);
            jedisPooled.xadd(outputStreamName, StreamEntryID.NEW_ENTRY,map);
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

    /*
    This method does two things every 30 seconds:
    1) Executes a call to Xtrim the OUTPUT_STREAM to the most recent 100 entries (assuming one is in use)
    2) looks for poisonpill Stream Events that are in pending state and sends them for processing/cleanup
    (you can find the resulting Hash record of the poisonpill by scanning for H:ProcessedEvent:::*
     */
    public void kickOffStreamReaping(long pendingMessageTimeout){
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, String> map1 = new HashMap<>();
                long counter = 0;
                while (true) {
                    try{ //pendingMessageTimeout  is modified by caller 'Main'
                        Thread.sleep(pendingMessageTimeout); // default is 60 seconds
                    }catch(InterruptedException ie){}
                    //try (JedisPooled streamReader = jedisPooled) {
                        if(shouldTrim) {
                            System.out.println(this.getClass().getName() + " -- Claiming and trimming loop...  attempt # " + counter + "   " + new Date());
                            //trim output stream of any events other than the most recent 100 events
                            XTrimParams xTrimParams = new XTrimParams().maxLen(100);
                            jedisPooled.xtrim(outputStreamName, xTrimParams);
                        }
                        try {
                            StreamPendingSummary streamPendingSummary = jedisPooled.xpending(forProcessingStreamName, consumerGroupName);
                            System.out.println("We have this many PENDING Entries: " + streamPendingSummary.getTotal());
                            if (streamPendingSummary.getTotal() > 0) {
                                String consumerID = (String) streamPendingSummary.getConsumerMessageCount().keySet().toArray()[0];
                                System.out.println("Min ID of the PENDING entries equals: " + streamPendingSummary.getMinId());
                                System.out.println("consumerID == " + consumerID);

                                List<StreamEntry> streamEntries = jedisPooled.xclaim(forProcessingStreamName,consumerGroupName,consumerID,30, XClaimParams.xClaimParams(),streamPendingSummary.getMinId());

                                if (streamEntries.size() > 0) {
                                    System.out.println("We got a live one: " + streamEntries.get(0).getID());
                                    StreamEntry discoveredPendingStreamEntry = streamEntries.get(0);
                                    if(((String)discoveredPendingStreamEntry.getFields()
                                            .get(payloadKeyName)).equalsIgnoreCase("poisonpill")) {
                                        Map<String,StreamEntry> poisonPayload = new HashMap();
                                        poisonPayload.put(forProcessingStreamName+":"+discoveredPendingStreamEntry.getID()+":"+consumerID,discoveredPendingStreamEntry);
                                        ((StreamEventMapProcessor)callbackTarget).processStreamEventMap((Map) poisonPayload);
                                    }else{  // we have a normal Pending message - just send it for regular processing
                                        Map<String,StreamEntry> entry = new HashMap();
                                        entry.put(forProcessingStreamName+":"+discoveredPendingStreamEntry.getID()+":"+consumerID,discoveredPendingStreamEntry);
                                        ((StreamEventMapProcessor)callbackTarget).processStreamEventMap(entry);
                                    }
                                    jedisPooled.xack(forProcessingStreamName, consumerGroupName, discoveredPendingStreamEntry.getID());
                                    if(shouldTrim) {
                                        jedisPooled.xdel(forProcessingStreamName, discoveredPendingStreamEntry.getID());
                                    }
                                }
                            }
                        }catch (Throwable t){
                            if(null == t.getMessage()){
                                System.out.println(this.getClass().getName()+" : There are no pending messages to clean up");
                                t.printStackTrace();
                            }else {
                                if (t.getMessage().equalsIgnoreCase("Cannot read the array length because \"bytes\" is null")) {
                                    //do nothing
                                    //System.out.println("Reaper Looking for Poison...> None Found this time which results in null: "+t.getMessage());
                                } else {
                                    t.printStackTrace();
                                }
                            }
                        }
                        counter++;
                        counter=counter%10; // reset counter to zero every 10 tries
                        if(counter==0) {
                            System.out.println("\t>>> Resetting StreamReaper Counter to 0 at " + new Date());
                        }
                }
            }
        }).start();
    }
}
