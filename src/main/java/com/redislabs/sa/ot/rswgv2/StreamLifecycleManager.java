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

    // only need to set TTL on the current active topic stream
    public static void setTTLSecondsForTopicActiveStream(JedisConnectionHelper helper,String topic,long secondsToLive){
        JedisPooled redis  = helper.getPooledJedis();
        String activeStream = setTopic(helper,topic,false);//only the consumers set the TTL
        redis.expire(activeStream,secondsToLive);
    }


    // make sure there is a redis LIST for the named TOPIC
    // If none exists make one and add the active StreamName to it
    // if none exists return the new to-be-activated StreamName
    // If it exists - the pre-existing active StreamName will be returned
    public static String setTopic(JedisConnectionHelper helper,String topic,boolean isPublisher){
        String response = "";//return the name of the topic created
        JedisPooled redis  = helper.getPooledJedis();
        Pipeline pipeline = helper.getPipeline();
        String newStreamName = "";
        if(!redis.exists(topic)){
            Response<List<String>> redisTime = pipeline.time();
            pipeline.sync();
            String ts = redisTime.get().get(0);
            System.out.println("[StreamLifecycleManager.setTopic()] Redis Time: --> "+ts);
            pipeline.close();
            newStreamName = topic+":"+ts;
            redis.lpush(topic,newStreamName);
        }else{
            if(isPublisher) {
                newStreamName = getLastAvailableStreamNameForTopic(helper, topic);
            }else{
                newStreamName = getFirstAvailableStreamNameForTopic(helper, topic);
            }
        }
        response = newStreamName;
        return response;
    }

    // This method checks a List for the earliest StreamName in existence
    // This is good for the Consumers
    // It also takes the opportunity to validate that the stream key named in the List exists
    // Stream Keys expire sometimes and it is important to be aware of that
    public static String getFirstAvailableStreamNameForTopic(JedisConnectionHelper helper,String topic){
        JedisPooled redis  = helper.getPooledJedis();
        long listIndex = redis.llen(topic)-1; // if length of list is 5: return 4 as the index (zero indexing)
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,nextCandidate)){
            redis.lpop(topic);
            listIndex = redis.llen(topic)-1;
            nextCandidate = redis.lindex(topic,listIndex);
        }
        //firstStreamName = redis.zrange(topic,0,1).get(0);
        return nextCandidate;
    }

    //this should be used by the Publishers:
    // in fact I don't know if we need it at all for the publishers
    // if a new publisher comes online it should just create a new Stream and carry on
    // Otherwise entries could be written back in time (to old keys already processed) unnecessarily
    public static String getLastAvailableStreamNameForTopic(JedisConnectionHelper helper,String topic){
        JedisPooled redis  = helper.getPooledJedis();
        long listIndex = 0;
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,nextCandidate)){ //nextCandidate is a Stream stored in the list or null
            redis.rpop(topic);
            listIndex = 0;
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
        String candidateStream = redis.lindex(topic,index-1);
        if(null!=candidateStream){
            if(foundResult(redis,candidateStream)){
                goodNextStreamname = candidateStream;
            }
        }
        return goodNextStreamname;
    }

    //  checks for the existsnce of a named Stream
    static boolean foundResult(JedisPooled redis, String streamName){
        boolean isGoodAndExists=false;
        try {
            isGoodAndExists = redis.exists(streamName);
            System.out.println("[StreamLifecycleManager.foundResult()] " + isGoodAndExists + " " + streamName);
        }catch(NullPointerException npe){/* ignore ? */}
        return isGoodAndExists;
    }

}
