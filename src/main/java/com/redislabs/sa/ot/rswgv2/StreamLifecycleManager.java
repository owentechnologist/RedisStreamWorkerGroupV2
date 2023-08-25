package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.*;

import java.util.List;

public class StreamLifecycleManager {

    public static String makeNewStreamForTopic(){
        Pipeline pipeline = Main.jedisConnectionHelper.getPipeline();
        JedisPooled redis = Main.jedisConnectionHelper.getPooledJedis();
        String topic = Main.TOPIC;
        Response<List<String>> redisTime = pipeline.time();
        pipeline.sync();
        String ts = redisTime.get().get(0);
        System.out.println("Redis Time: --> "+ts);
        pipeline.close();
        String newStreamName = topic+":"+ts;
        redis.lpush(topic,newStreamName);
        return newStreamName;
    }

    // only need to set TTL on the current active topic stream
    // when it goes away - then there is a chance to set TTL on the next Active Topic stream
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
            System.out.println("Redis Time: --> "+ts);
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
        long listIndex = redis.llen(topic)-1;
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,topic,nextCandidate)){
            redis.lpop(topic);
            listIndex = redis.llen(topic)-1;
            nextCandidate = redis.lindex(topic,listIndex);
        }
        //firstStreamName = redis.zrange(topic,0,1).get(0);
        return nextCandidate;
    }

    //this should be used by the Publishers:
    // Otherwise entries could be written back in time (to old keys already processed) unnecessarily
    public static String getLastAvailableStreamNameForTopic(JedisConnectionHelper helper,String topic){
        JedisPooled redis  = helper.getPooledJedis();
        long listIndex = 0;
        String nextCandidate = redis.lindex(topic,listIndex);
        while(!foundResult(redis,topic,nextCandidate)){
            redis.rpop(topic);
            listIndex = 0;
            nextCandidate = redis.lindex(topic,listIndex);
        }
        return nextCandidate;
    }

    static boolean foundResult(JedisPooled redis, String topic,String streamName){
        boolean isFirstAndExists=false;
        try {
            isFirstAndExists = redis.exists(streamName);
            System.out.println("SLM.foundResult() " + isFirstAndExists + " " + streamName);
        }catch(NullPointerException npe){/* ignore ? */}
        return isFirstAndExists;
    }

}
