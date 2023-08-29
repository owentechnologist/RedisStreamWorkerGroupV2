package com.redislabs.sa.ot.rswgv2;

import com.redislabs.sa.ot.util.JedisConnectionHelper;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.Transaction;

import java.util.HashMap;

public class TopicGovernor {

    //it is important to use routing values in the names of your topics like:
    // TopicA:{1}
    // where {1} is the routing value - this will allow for the renaming strategy to work
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

    public void watchOverTopics(JedisConnectionHelper jedisConnectionHelper) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                //FIXME: At some point the governor could decide it is time to put a TTL on a stream in a topic
            }
        }).start();
    }

}
