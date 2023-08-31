package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.*;

import java.util.List;

public class StreamLifecycleManager {

    public static String makeNewStreamForTopic(){
        Pipeline pipeline = Main.jedisConnectionHelper.getPipeline();
        JedisPooled redis = Main.jedisConnectionHelper.getPooledJedis();
        String topic = Main.TOPIC;
        Response<List<String>> redisResponse = pipeline.time();
        pipeline.sync();
        pipeline.close();
        String redisTime = redisResponse.get().get(0);
        System.out.println("[StreamLifecycleManager.makeNewStreamForTopic()] Redis Time: --> "+redisTime);
        String newStreamName = topic+":"+redisTime;
        redis.lpush(topic,newStreamName);
        return newStreamName;
    }

    // only need to set TTL on the current oldest topic stream
    public static String setTTLSecondsForTopicOldestStream(JedisConnectionHelper helper, String topic, long secondsToLive){
        JedisPooled redis  = helper.getPooledJedis();
        String oldestStream = cleanAndGetOldestAvailableStreamNameForTopic(helper,topic);
        redis.expire(oldestStream,secondsToLive);
        return oldestStream;
    }


    // make sure there is a redis LIST for the named TOPIC
    // If none exists make one and add the active StreamName to it
    // if none exists return the new to-be-activated StreamName
    // If it exists - the pre-existing active StreamName will be returned
    public static String establishTopicForConsumer(JedisConnectionHelper helper, String topic){
        String response = "";//return the name of the topic created
        JedisPooled redis  = helper.getPooledJedis();
        Pipeline pipeline = helper.getPipeline();
        String newStreamName = "";
        if(!redis.exists(topic)){
            Response<List<String>> redisTime = pipeline.time();
            pipeline.sync();
            String ts = redisTime.get().get(0);
            System.out.println("[StreamLifecycleManager.establishTopicForConsumer()] Redis Time: --> "+ts);
            pipeline.close();
            newStreamName = topic+":"+ts;
            redis.lpush(topic,newStreamName);
        }else{
            newStreamName = cleanAndGetOldestAvailableStreamNameForTopic(helper, topic);
        }
        response = newStreamName;
        return response;
    }

    // make sure there is a redis LIST for the named TOPIC
    // If none exists make one and add the active StreamName to it
    // if none exists return the new to-be-activated StreamName
    // If it exists - the pre-existing active StreamName will be returned
    public static String establishTopicForPublisher(JedisConnectionHelper helper, String topic){
        String response = "";//return the name of the topic created
        JedisPooled redis  = helper.getPooledJedis();
        Pipeline pipeline = helper.getPipeline();
        String newStreamName = "";
        if(!redis.exists(topic)){
            Response<List<String>> redisTime = pipeline.time();
            pipeline.sync();
            String ts = redisTime.get().get(0);
            System.out.println("[StreamLifecycleManager.establishTopicForPublisher()] Redis Time: --> "+ts);
            pipeline.close();
            newStreamName = topic+":"+ts;
            redis.lpush(topic,newStreamName);
        }else{
            newStreamName = cleanAndGetNewestAvailableStreamNameForTopic(helper, topic);
        }
        response = newStreamName;
        return response;
    }


    // This method checks the topic (List) for the earliest StreamName in existence
    // This is good for the Publishers who want to publish to the newest stream in a topic
    // It also takes the opportunity to validate that the stream key named in the List exists
    // Stream Keys expire sometimes and it is important to be aware of that
    public static String cleanAndGetNewestAvailableStreamNameForTopic(JedisConnectionHelper helper,String topic){
        JedisPooled redis  = helper.getPooledJedis();
        long listIndex = 0; //(zero indexing - zero is always the last [newest] one written)
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,nextCandidate)){
            System.out.println("[StreamLifecycleManager.cleanAndGetNewestAvailableStreamNameForTopic()] Expected stream does not exist for topic: "+topic);
            redis.lpop(topic);
            listIndex = redis.llen(topic)-1;
            nextCandidate = redis.lindex(topic,listIndex);
        }
        return nextCandidate;
    }

    //this should be used by the TopicGovernor:
    // if a new publisher comes online it should just create a new Stream and carry on
    // Otherwise entries could be written back in time (to old keys already processed) unnecessarily
    public static String cleanAndGetOldestAvailableStreamNameForTopic(JedisConnectionHelper helper, String topic){
        JedisPooled redis  = helper.getPooledJedis();
        long listIndex = redis.llen(topic)-1;
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,nextCandidate)){ //nextCandidate is a Stream stored in the list or null
            redis.rpop(topic); // this should eliminate the oldest stream
            listIndex = redis.llen(topic)-1;
            nextCandidate = redis.lindex(topic,listIndex);
        }
        return nextCandidate;
    }

    // Consumers can use this method to move to the next Stream in the Topic
    // This allows for movement across streams within the middle of the list
    // uses LPOS and the stream already/last processed to find what the next stream index should be
    public static String getNextAvailableStreamNameForTopic(String topic,String previousStream){
        String goodNextStreamname = "";
        JedisPooled redis = Main.jedisConnectionHelper.getPooledJedis();
        long index = redis.lpos(topic,previousStream);
        // because of LPUSH behavior
        // next oldest stream in topic will be 1 index place lower (closer to the beginning of the List)
        // NB: if you are at the end of the list and subtract 1 - you are put at the TOP of the list
        index = index-1; // we want to get the next Stream in the list
        if(index<0){index=0;} //should keep us from endless looping
        String candidateStream = redis.lindex(topic,index);
        if(null!=candidateStream){
            if(foundResult(redis,candidateStream)){
                goodNextStreamname = candidateStream;
            }
        }
        return goodNextStreamname;
    }


    //  checks for the existence of a named Stream
    static boolean foundResult(JedisPooled redis, String streamName){
        boolean isGoodAndExists=false;
        try {
            isGoodAndExists = redis.exists(streamName);
            System.out.println("[StreamLifecycleManager.foundResult()] " + isGoodAndExists + " " + streamName);
        }catch(NullPointerException npe){/* ignore ? */}
        return isGoodAndExists;
    }

}
