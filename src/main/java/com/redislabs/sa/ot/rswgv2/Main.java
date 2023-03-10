package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelper;
import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import java.util.ArrayList;
import java.util.Arrays;

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
    public static String RESULTS_STREAM_NAME = "X:PROCESSED_EVENTS{1}";
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

    public static void main(String [] args){
        ArrayList<String> argList = null;
        String host = "localhost";
        int port = 6379;
        String userName = "default";
        String password = "";

        if(args.length>0) {
            argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--verbose")) {
                int argIndex = argList.indexOf("--verbose");
                VERBOSE = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            if (argList.contains("--streamname")) {
                int argIndex = argList.indexOf("--streamname");
                STREAM_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--resultsstreamname")) {
                int argIndex = argList.indexOf("--resultsstreamname");
                RESULTS_STREAM_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--consumergroupname")) {
                int argIndex = argList.indexOf("--consumergroupname");
                CONSUMER_GROUP_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--host")) {
                int argIndex = argList.indexOf("--host");
                host = argList.get(argIndex + 1);
            }
            if (argList.contains("--port")) {
                int argIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(argIndex + 1));
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
            }
            if (argList.contains("--startreaper")) {
                int argIndex = argList.indexOf("--startreaper");
                IS_REAPER_ACTIVE = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            if (argList.contains("--writerbatchsize")) {
                int argIndex = argList.indexOf("--writerbatchsize");
                WRITER_BATCH_SIZE = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--writersleeptime")) {
                int argIndex = argList.indexOf("--writersleeptime");
                WRITER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--addondeltaforworkername")) {
                int argIndex = argList.indexOf("--addondeltaforworkername");
                ADD_ON_DELTA_FOR_WORKER_NAME = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyentries")) {
                int argIndex = argList.indexOf("--howmanyentries");
                HOW_MANY_ENTRIES = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--workersleeptime")) {
                int argIndex = argList.indexOf("--workersleeptime");
                WORKER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
            }
        }
        JedisConnectionHelper jedisConnectionHelper = new JedisConnectionHelper(host,port,userName,password);
        //testConnection(jedisConnectionHelper);

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
                    new RedisStreamWorkerGroupHelper(STREAM_NAME, jedisConnectionHelper.getPooledJedis(),VERBOSE);
            redisStreamWorkerGroupHelper.createConsumerGroup(CONSUMER_GROUP_NAME);
            for(int w=0;w<NUMBER_OF_WORKER_THREADS;w++){
                StreamEventMapProcessor processor =
                        new StreamEventMapProcessorToStream()
                                .setJedisConnectionHelper(jedisConnectionHelper.getPooledJedis())
                                .setPayloadKeyName(PAYLOAD_KEY_NAME)
                                .setSleepTime(WORKER_SLEEP_TIME)
                                .setOutputStreamName(RESULTS_STREAM_NAME)
                                .setVerbose(VERBOSE);
                String workerName = "worker"+(w+ADD_ON_DELTA_FOR_WORKER_NAME);
                redisStreamWorkerGroupHelper.namedGroupConsumerStartListening(workerName,processor);
            }
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

