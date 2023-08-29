package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelper;
import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.time.format.DateTimeFormatter;

import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.*;

/**
 * The program demonstrates writing and processing events using Redis Streams
 * A writer writes X events/entries to one stream
 * Some number of workers (belonging to a worker group) consume those entries and process them
 * The processed entries are written to a separate stream
 *
 * The default settings use:
 * * A stream name of "X:FOR_PROCESSING{1}"
 * * A stream name of "X:PROCESSED_EVENTS{1}"
 * * 1 Writer that writes 10000 entries in batches of 200 entries (with 50 millisecond pauses between each batch)
 * * A worker group of 2 workers that process the entries 1 at a time and sleep 50 milliseconds between each one
 *
 * To run the program with the default settings (supplying the host and port for Redis) do:
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host myhost.com --port 10000"
 *
 * The programmatic factors that determine the rate of processing are:
 * A) the number of workers in a worker group for each stream
 * B) the sleep time given to the workers between entry processing work
 * C) the sleep time given to the writers between batches of writes of entries
 * D) the batch size of entries given to the writers
 *
**/
public class Main {

    public static String STREAM_NAME = "X:FOR_PROCESSING{1}";
    public static String RESULTS_KEY_NAME = "X:PROCESSED_EVENTS{1}";
    public static int NUMBER_OF_WORKER_THREADS = 2;
    public static int WORKER_SLEEP_TIME = 50;//milliseconds
    public static boolean IS_REAPER_ACTIVE=false;
    public static int WRITER_SLEEP_TIME = 50;//milliseconds
    public static int HOW_MANY_ENTRIES = 10000;
    public static int WRITER_BATCH_SIZE = 200;
    public static String CONSUMER_GROUP_NAME = "GROUP_ALPHA";
    public static String PAYLOAD_KEY_NAME = "stringOffered";
    public static int ADD_ON_DELTA_FOR_WORKER_NAME = 0;
    public static boolean VERBOSE = false;
    public static boolean SHOULD_TRIM_STREAM = false;
    public static long STREAM_TTL_SECONDS = 300;
    public static String TOPIC = "X:FOR_PROCESSING{1}";
    public static boolean CONSUMER_RESPONSE_IS_A_STREAM = true;
    public static String STREAM_READ_START_POINT_IN_STREAM = String.valueOf(StreamEntryID.LAST_ENTRY); // This equals "$"
    public static int connectionPoolSize = 100;
    public static JedisConnectionHelper jedisConnectionHelper = null;
    public static long MAX_STREAM_LENGTH = 1000000;
    public static boolean IS_TOPIC_GOVERNOR = false;

    public static void main(String [] args){
        ArrayList<String> argList = null;
        String host = "localhost";
        int port = 6379;
        String userName = "default";
        String password = "";
        String startStatus = "\nLaunching: \nThe local time is now "+java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMMM dd, yyyy, H:mm:ss"));

        if(args.length>0) {
            argList = new ArrayList<>(Arrays.asList(args));
            // Playing with the concept of a logical TOPIC that is implemented with multiple streams
            // As time goes by the stream names change and old ones expire due to TTL / expiration
            // the topic governor is responsible for initiating the first stream in a topic
            // adding it to the Topic list object in Redis and eventually rotating / renaming the streams
            if (argList.contains("--topicgovernor")) {
                int argIndex = argList.indexOf("--topicgovernor");
                IS_TOPIC_GOVERNOR = Boolean.parseBoolean(argList.get(argIndex + 1));
                if(IS_TOPIC_GOVERNOR) {
                    startStatus += "\n***** TopicGovernor ( WILL NOT WRITE OR CONSUME EVENTS ) ******";
                }
            }
            if (argList.contains("--topic")) {
                int argIndex = argList.indexOf("--topic");
                TOPIC = argList.get(argIndex + 1);
                startStatus+="\nTopic name is - "+TOPIC;
            }
            if (argList.contains("--verbose")) {
                int argIndex = argList.indexOf("--verbose");
                VERBOSE = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            // if using TOPIC - the streamname will be given from the StreamLifecycleManager not from this
            if (argList.contains("--streamname")) {
                int argIndex = argList.indexOf("--streamname");
                STREAM_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--resultskeyname")) {
                int argIndex = argList.indexOf("--resultskeyname");
                RESULTS_KEY_NAME = argList.get(argIndex + 1);
                startStatus+="\nResults key name is - "+ RESULTS_KEY_NAME;
            }
            if (argList.contains("--consumergroupname")) {
                int argIndex = argList.indexOf("--consumergroupname");
                CONSUMER_GROUP_NAME = argList.get(argIndex + 1);
                startStatus+="\nConsumer group name is - "+CONSUMER_GROUP_NAME;
            }
            if (argList.contains("--host")) {
                int argIndex = argList.indexOf("--host");
                host = argList.get(argIndex + 1);
            }
            if (argList.contains("--port")) {
                int argIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--connectionpoolsize")) {
                int argIndex = argList.indexOf("--connectionpoolsize");
                connectionPoolSize = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--username")) {
                int argIndex = argList.indexOf("--username");
                userName = argList.get(argIndex + 1);
            }
            if (argList.contains("--password")) {
                int argIndex = argList.indexOf("--password");
                password = argList.get(argIndex + 1);
            }
            if (argList.contains("--howmanyworkers")) {
                int argIndex = argList.indexOf("--howmanyworkers");
                NUMBER_OF_WORKER_THREADS = Integer.parseInt(argList.get(argIndex + 1));
                if(NUMBER_OF_WORKER_THREADS>0){
                    startStatus+="\n# members in group is - "+NUMBER_OF_WORKER_THREADS;
                }
            }
            if (argList.contains("--startreaper")) {
                int argIndex = argList.indexOf("--startreaper");
                IS_REAPER_ACTIVE = Boolean.parseBoolean(argList.get(argIndex + 1));
                if(IS_REAPER_ACTIVE) {
                    startStatus += "\nThis is a reaper instance ";
                }
            }
            if (argList.contains("--shouldtrimstream")) {
                int argIndex = argList.indexOf("--shouldtrimstream");
                SHOULD_TRIM_STREAM = Boolean.parseBoolean(argList.get(argIndex + 1));
                if(SHOULD_TRIM_STREAM) {
                    startStatus += "\nWorkers will delete processed events from topic ";
                }
            }
            if (argList.contains("--writerbatchsize")) {
                int argIndex = argList.indexOf("--writerbatchsize");
                WRITER_BATCH_SIZE = Integer.parseInt(argList.get(argIndex + 1));
                startStatus+="\n{Publishing in batches of "+WRITER_BATCH_SIZE;
            }
            if (argList.contains("--writersleeptime")) {
                int argIndex = argList.indexOf("--writersleeptime");
                WRITER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
                startStatus+="\nPublisher will sleep this many milliseconds between each batch: "+WRITER_SLEEP_TIME;
            }
            if (argList.contains("--addondeltaforworkername")) {
                int argIndex = argList.indexOf("--addondeltaforworkername");
                ADD_ON_DELTA_FOR_WORKER_NAME = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyentries")) {
                int argIndex = argList.indexOf("--howmanyentries");
                HOW_MANY_ENTRIES = Integer.parseInt(argList.get(argIndex + 1));
                startStatus+="\nTotal number this publisher will write to topic is "+HOW_MANY_ENTRIES;
            }
            if (argList.contains("--activestreamttlseconds")) {
                int argIndex = argList.indexOf("--activestreamttlseconds");
                STREAM_TTL_SECONDS = Long.parseLong(argList.get(argIndex + 1));
                startStatus+="\nSeconds this consumer is setting TTL on the stream mapped to topic: "+STREAM_TTL_SECONDS;
            }
            if (argList.contains("--maxstreamlength")) { // after this length, switch to a new stream
                int argIndex = argList.indexOf("--maxstreamlength");
                MAX_STREAM_LENGTH = Long.parseLong(argList.get(argIndex + 1));
                startStatus+="\nPublisher will create a new stream every time one has more entries than "+MAX_STREAM_LENGTH;
            }
            if (argList.contains("--workersleeptime")) {
                int argIndex = argList.indexOf("--workersleeptime");
                WORKER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
                startStatus+="\nWorker sleeps between processing messages for "+WORKER_SLEEP_TIME;
            }
            if (argList.contains("--streamreadstart")) { //This is used when new consumers come online
                int argIndex = argList.indexOf("--streamreadstart");
                STREAM_READ_START_POINT_IN_STREAM = argList.get(argIndex + 1);
                startStatus+="\nAll consumers will begin reading from the target stream using: "+ STREAM_READ_START_POINT_IN_STREAM;
            }
            if (argList.contains("--consumerresponseisastream")) { //This is used when new consumers come online
                int argIndex = argList.indexOf("--consumerresponseisastream");
                CONSUMER_RESPONSE_IS_A_STREAM = Boolean.parseBoolean(argList.get(argIndex + 1));
                if(CONSUMER_RESPONSE_IS_A_STREAM) {
                    startStatus += "\nThese consumers will write Stream responses to: " + RESULTS_KEY_NAME;
                }else{
                    startStatus += "\nThese consumers will increment this key as a counter to show work is being done: " + RESULTS_KEY_NAME;
                }
            }
        }
        jedisConnectionHelper = new JedisConnectionHelper(host,port,userName,password,connectionPoolSize);
        System.out.println(startStatus+"\n");
        if(IS_TOPIC_GOVERNOR){
            System.out.println("This version of the application is responsible" +
                    " for creating the policy for a topic and creates the first stream for that topic"
                    +"\n eventually it will also rename streams within a topic according to the policy");
            TopicGovernor governor = new TopicGovernor();
            governor.makeFirstStreamForTopic(TOPIC,jedisConnectionHelper);
            System.exit(0);
        }
        //testConnection(jedisConnectionHelper);
        if(argList.contains("--topic")){
            // the caller expects us to have multiple StreamNames sharing the responsibility of a single topic
            // we will put the names into a Redis LIST named for the TOPIC value
            // The list will keep track of the names of the streams created for that topic
            if(HOW_MANY_ENTRIES>0) { // we are a publisher find the most recent stream written to:
                STREAM_NAME = StreamLifecycleManager.setTopic(jedisConnectionHelper, TOPIC, true);
            }else{ // we are a consumer find the oldest stream for this topic:
                STREAM_NAME = StreamLifecycleManager.setTopic(jedisConnectionHelper, TOPIC, false);
            }
        }
        if(argList.contains("--activestreamttlseconds")){
            StreamLifecycleManager.setTTLSecondsForTopicActiveStream(jedisConnectionHelper,TOPIC,STREAM_TTL_SECONDS);
        }
        if(HOW_MANY_ENTRIES>0){ //we will be writing some entries
            StreamWriter streamWriter =
                    new StreamWriter(STREAM_NAME,jedisConnectionHelper.getPipeline())
                            .setBatchSize(WRITER_BATCH_SIZE)
                            .setPayloadKeyName(PAYLOAD_KEY_NAME)
                            .setSleepTime(WRITER_SLEEP_TIME)
                            .setTotalNumberToWrite(HOW_MANY_ENTRIES);
            streamWriter.kickOffStreamEvents();
        }
        if(NUMBER_OF_WORKER_THREADS>0){
            RedisStreamWorkerGroupHelper redisStreamWorkerGroupHelper =
                    new RedisStreamWorkerGroupHelper(TOPIC,STREAM_NAME, jedisConnectionHelper,VERBOSE);
            redisStreamWorkerGroupHelper.createConsumerGroup(CONSUMER_GROUP_NAME, STREAM_READ_START_POINT_IN_STREAM);
            for(int w=0;w<NUMBER_OF_WORKER_THREADS;w++){
                StreamEventMapProcessor processor = null;
                if(CONSUMER_RESPONSE_IS_A_STREAM) {
                    processor =
                            new StreamEventMapProcessorToStream()
                                    .setJedisConnectionHelper(jedisConnectionHelper)
                                    .setPayloadKeyName(PAYLOAD_KEY_NAME)
                                    .setSleepTime(WORKER_SLEEP_TIME)
                                    .setOutputStreamName(RESULTS_KEY_NAME)
                                    .setVerbose(VERBOSE);
                }else{
                    processor = new StreamEventMapProcessorCounterIncr()
                            .setJedisConnectionHelper(jedisConnectionHelper)
                            .setPayloadKeyName(PAYLOAD_KEY_NAME)
                            .setSleepTime(WORKER_SLEEP_TIME)
                            .setOutputStreamName(RESULTS_KEY_NAME)
                            .setVerbose(VERBOSE);
                }
                String workerName = "worker"+(w+ADD_ON_DELTA_FOR_WORKER_NAME);
                redisStreamWorkerGroupHelper.namedGroupConsumerStartListening(workerName,processor,SHOULD_TRIM_STREAM);
            }
        }
        if(IS_REAPER_ACTIVE){
            StreamReaper streamReaper = new StreamReaper().setForProcessingStreamKeyName(STREAM_NAME)
                    .setOutputStreamName(RESULTS_KEY_NAME)
                    .setShouldTrimOutPutStream(SHOULD_TRIM_STREAM)//FIXME: reaper applies this to both source and output streams
                    .setConsumerGroupName(CONSUMER_GROUP_NAME)
                    .setSleepTime(WORKER_SLEEP_TIME)
                    .setJedisConnectionHelper(jedisConnectionHelper)
                    .setPayloadKeyName(PAYLOAD_KEY_NAME)
                    .setVerbose(VERBOSE);
            streamReaper.setCallbackTarget(streamReaper);
            System.out.println("\n\tStarting StreamReaper (to collect and process Pending entries\n\n");
            streamReaper.kickOffStreamReaping(6000);
        }
    }

    private static void testConnection(JedisConnectionHelper helper){
        helper.getPooledJedis().append("testKey","test ");
        System.out.println(helper.getPooledJedis().get("testKey"));
        for(int x = 0;x<1000;x++){
            if(x%100==0){
                System.out.println(helper.getPooledJedis().del("testKey"));
            }else{
                helper.getPooledJedis().append("testKey","test ");
                System.out.println(helper.getPooledJedis().get("testKey"));
            }
        }
    }
}

