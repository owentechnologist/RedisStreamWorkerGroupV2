package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TopicGovernor {
    public static final String TOPIC_POLICY_EXPIRY_STARTED_ATTRIBUTE_NAME = "DO_NOT_EDIT:_expiry_of_oldest_stream_in_topic_has_begun";
    public static final String TOPIC_POLICY_TARGET_STREAM_FOR_TTL_ATTRIBUTE_NAME = "DO_NOT_EDIT:_target_stream_for_ttl";
    public static final String TOPIC_POLICY_TTL_ATTRIBUTE_NAME = "ttl_seconds";
    public static final String TOPIC_POLICY_SHOULD_EXPIRE_ATTRIBUTE_NAME = "should_expire";
    public static final String TOPIC_POLICY_REDIS_TIME_START_TTL_COUNTDOWN_BEGAN = "DO_NOT_EDIT:_timestamp_of_start_ttl_countdown";
    public static final String TOPIC_POLICY_KEYNAME_PREFIX = "topic_policy:";//concatenate the topic name to this

    private ArrayList<String> topics = null;

    //FIXME: August 29th 2023 refactor or remove this method as it is no longer needed
    //  If planning to rename streams (not the best idea)
    // if used for renaming:
    // it is important to use routing values in the names of your topics like:
    // TopicA:{1}
    // where {1} is the routing value - this will allow for the renaming strategy to work
    //
    //NB this is a demonstration of how to create an empty stream with no Consumers
    public void makeFirstStreamForTopic(String topic, JedisConnectionHelper jedisConnectionHelper){
        JedisPooled redis = Main.jedisConnectionHelper.getPooledJedis();
        if(redis.exists(topic)){
            throw new RuntimeException("The topic has already been created - check your process - try a different name");
        }
        String activeStreamName = topic+":first";
        HashMap<String,String> map = new HashMap<>();
        map.put("0","0");
        Transaction transaction = jedisConnectionHelper.getTransaction();
        transaction.lpush(topic,activeStreamName);
        transaction.xadd(activeStreamName, StreamEntryID.NEW_ENTRY,map);
        transaction.xtrim(activeStreamName,0,false);
        transaction.exec();
    }

    //Sets the topics this instance of Governor will manage:
    public void setTopics(ArrayList<String> topics){
        this.topics = topics;
    }

    //Should establish a per topic policy that is read by this Governor in case different TTL are needed
    //once every sleepTime, this service will look for changes to the policies for the topics
    //when it finds a policy, it will use it to establish a TTL for the oldest stream in the LIST
    //FIXME: (not in this class, but in general) test what happens to the topic/list when a stream expires and is evicted
    // note that upon expiry & eviction any consumer group associated will be expired and evicted as well
    public void manageTopics(JedisConnectionHelper jedisConnectionHelper,long sleepTimeSeconds) {
        if(topics == null){
            throw new RuntimeException("Programmer error --> You need to call setTopics() before you try to manage them.");
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                JedisPooled redis = jedisConnectionHelper.getPooledJedis();
                Pipeline pipeline = jedisConnectionHelper.getPipeline();
                while(true) {
                    //At some point the governor could decide it is time to put a TTL on a stream in a topic
                    //the decision will be made based on
                    for (String topic : topics) {
                        if (null != topic) {
                            boolean shouldExpire = false;
                            boolean isExpiryStarted = false;
                            long ttlSeconds = 60;
                            try {
                                Response<List<String>> redisResponse = pipeline.time();
                                pipeline.sync();
                                pipeline.close();
                                String redisTime = redisResponse.get().get(0);
                                long timeCountdownTTLBegan = 0;
                                String targetStream = "";

                                if (redis.exists(TOPIC_POLICY_KEYNAME_PREFIX + topic)) {
                                    System.out.println("[TopicGovernor.manageTopics()] found policy: "+TOPIC_POLICY_KEYNAME_PREFIX + topic);
                                    System.out.println("[TopicGovernor.manageTopics()] If you want to expire a key using this TopicGovernor - modify the attributes in that policy in Redis");
                                    shouldExpire = Boolean.parseBoolean(redis.hget(TOPIC_POLICY_KEYNAME_PREFIX + topic, TOPIC_POLICY_SHOULD_EXPIRE_ATTRIBUTE_NAME));
                                    ttlSeconds = Long.parseLong(redis.hget(TOPIC_POLICY_KEYNAME_PREFIX + topic, TOPIC_POLICY_TTL_ATTRIBUTE_NAME));
                                    isExpiryStarted = Boolean.parseBoolean(redis.hget(TOPIC_POLICY_KEYNAME_PREFIX + topic, TOPIC_POLICY_EXPIRY_STARTED_ATTRIBUTE_NAME));
                                    timeCountdownTTLBegan = Long.parseLong(redis.hget(TOPIC_POLICY_KEYNAME_PREFIX + topic,TOPIC_POLICY_REDIS_TIME_START_TTL_COUNTDOWN_BEGAN));

                                    if (shouldExpire && (!isExpiryStarted)) { // set the countdown clock and initiate TTL for oldest stream
                                        System.out.println("Setting TTL on " + topic);
                                        //initiate TTL on oldest stream:
                                        targetStream = StreamLifecycleManager.setTTLSecondsForTopicOldestStream(jedisConnectionHelper, topic, ttlSeconds);
                                        redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_EXPIRY_STARTED_ATTRIBUTE_NAME,"true");
                                        redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_REDIS_TIME_START_TTL_COUNTDOWN_BEGAN,redisTime);
                                        redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_TARGET_STREAM_FOR_TTL_ATTRIBUTE_NAME,targetStream);
                                    }
                                    // SET 999999999 as TTL and so on for next Stream in Topic - until some policy setting mechanism overrides it:
                                    if(shouldExpire && isExpiryStarted){ //check for and act on age of TTL for oldest Stream in topic
                                        System.out.println("\n*** [TopicGovernor.manageTopics()] Checking Expiry Status for "+topic);
                                        System.out.println("\n*** [TopicGovernor.manageTopics()] redisTime - timeCountdownBegan == "+(Long.parseLong(redisTime)-timeCountdownTTLBegan));
                                        if((Long.parseLong(redisTime)-timeCountdownTTLBegan) > (ttlSeconds)){
                                            String nextOldest = StreamLifecycleManager.cleanAndGetOldestAvailableStreamNameForTopic(jedisConnectionHelper,topic);
                                            System.out.println("\n*** [TopicGovernor.manageTopics()] Expiry complete... new oldest Stream is "+nextOldest+
                                            "\n---->>>  NB: This program is not designed to force the next expiration TTL to trigger" +
                                                    "\n---->>> change the should_expire flag to true in the "+TOPIC_POLICY_KEYNAME_PREFIX+topic+" to trigger the next TTL");
                                            redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_EXPIRY_STARTED_ATTRIBUTE_NAME,"false");
                                            redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_REDIS_TIME_START_TTL_COUNTDOWN_BEGAN,"999999999");
                                            redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_TARGET_STREAM_FOR_TTL_ATTRIBUTE_NAME,nextOldest);
                                            // Who decides when the next Expiry countdown begins? until we know we reset this for the next stream in topic:
                                            redis.hset(TOPIC_POLICY_KEYNAME_PREFIX+topic,TOPIC_POLICY_SHOULD_EXPIRE_ATTRIBUTE_NAME,"false");
                                            //assume same ttl setting applies to next stream for this topic so we don't reset that value
                                        }
                                    }
                                } else {//create a default policy object to be nice in case someone looks for it
                                    System.out.println("\n*** [TopicGovernor.manageTopics()]  No TopicPolicy Found for topic: " + topic +
                                            " Creating a new Topic policy in redis called " + TOPIC_POLICY_KEYNAME_PREFIX + topic);
                                    HashMap<String, String> policyMap = new HashMap<>();
                                    policyMap.put(TOPIC_POLICY_SHOULD_EXPIRE_ATTRIBUTE_NAME, "false");
                                    policyMap.put(TOPIC_POLICY_TTL_ATTRIBUTE_NAME, "999999999");
                                    policyMap.put(TOPIC_POLICY_EXPIRY_STARTED_ATTRIBUTE_NAME,"false");
                                    policyMap.put(TOPIC_POLICY_REDIS_TIME_START_TTL_COUNTDOWN_BEGAN,"999999999");
                                    policyMap.put(TOPIC_POLICY_TARGET_STREAM_FOR_TTL_ATTRIBUTE_NAME,"NONE_SET");
                                    redis.hset(TOPIC_POLICY_KEYNAME_PREFIX + topic, policyMap);
                                }
                            } catch (Throwable t) {
                                System.out.println("[TopicGovernor.manageTopics()] " + t.getMessage() + " \nHad an issue with topic -> " +
                                        topic + " ttlSeconds -> " + ttlSeconds + " shouldExpire -> " + shouldExpire +
                                        "\n ... Does the topic in question exist? " +
                                        "\n(it will be a List of streams in Redis created by a Publisher)");
                                try{
                                    //in case this error was caused by network or pooling issue...
                                    Thread.sleep(1000);
                                    redis = jedisConnectionHelper.getPooledJedis();
                                    pipeline = jedisConnectionHelper.getPipeline();
                                }catch(Throwable another){
                                    another.printStackTrace();
                                }
                            }
                        }
                    }
                    //rest up until next iteration:
                    try{
                        Thread.sleep(sleepTimeSeconds*1000); //sleep takes milliseconds as an argument
                    }catch(Throwable t){}//do nothing if sleep fails
                }
            }
        }).start();
    }

}
