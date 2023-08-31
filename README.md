## This program demonstrates writing and processing events using Redis Streams using Jedis 4.3.1

### A bunch has changed this August... I added the concept of a Topic
### Implemented as a List in Redis, it keeps track of all related streams

There are three main versions of this program you can run:
1. Publisher
2. Consumer
3. TopicGovernor

Running each one in a separate shell allows you to clearly see the flow as
publishers move from stream to stream within a topic and consumers do the same
and topicGovernor allows for easy expiration (TTL) configuration and execution 
through the changing of attributes in a TopicPolicy object stored in redis.

![simplified workflow](StreamsInTopicsForScaling.png)
#### You can now set up a publisher that writes multiple streams to a topic
These are the arguments that trigger publishing:  
```
--topic someInterestingTopicA --ispublisher true --howmanyentries 10000000 --maxstreamlength 1000000
``` 
#### Workers / ConsumerGroup Members now also use the --topic argument instead of a --streamname
#### The workers now move sequentially from Stream to Stream within a Topic: processing all entries
#### Starting with the oldest Stream

#### - A writer / Publisher writes X events/entries to the streams in a topic
#### - Some number of workers (belonging to a worker group) consume those entries and process them
#### - The processed entries are written to a separate stream ... OR: they are simply counted, and the count is stored in a Redis String
### to get the 'just counted' behavior use:  
--consumerresponseisastream false

#### You may also want to provide the name of the string key used to store the count for this group:
```
--consumerresponseisastream false --resultskeyname topicA:workerGroup2:ResultCount
```

### Example start of a Publisher:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --topic TopicA:{1} --ispublisher true --howmanyentries 1000000 --maxstreamlength 50000 --writerbatchsize 10 --writersleeptime 10 --isconsumer false"
```
### Example start of a Consumer:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.20 --port 10400 --topic TopicA:{1} --ispublisher false --howmanyworkers 2 --shouldtrimstream false --workersleeptime 10 --streamreadstart 0-0 --resultskeyname consumer:count:TopicA:OT_3 --consumerresponseisastream false  --consumergroupname OT_3"
```

#### This program also has a TopicGovernor mode
#### - running as a TopicGovernor allows setting TTL on the oldest stream in a topic

### Example start of a TopicGovernor:
``` 
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.20 --port 10400 --topic TopicA:{1} --ispublisher false --isconsumer false --isgovernor true --governorsleepsecs 60"
```

You can run without the
```
--topic <sometopic>
```
argument if you do not want that feature.

The default settings have workers that write results to a second stream and it operates at around 100 ops/second and use:

- A **work to be done** stream name of "X:FOR_PROCESSING{1}"
- A **work completed** stream name of "X:PROCESSED_EVENTS{1}"
- One writer that writes 10000 entries in batches of 200 entries (with 50 millisecond pauses between each batch)
- A worker group of 2 workers that process the entries 1 at a time and sleep 50 milliseconds between each one

* To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.20 --port 10400 --ispublisher true --howmanyentries 10000 --isconsumer true --howmanyworkers 2"
```

* To run the program much, much, much faster you can use the following command:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host 192.168.1.20 --port 10400 --ispublisher true --howmanyentries 10000 --isconsumer true --howmanyworkers 20"
```


The programmatic factors that determine the rate of processing are:
- A) the number of workers in a worker group for each stream
- B) the sleep time given to the workers between entry processing work
- C) the sleep time given to the writers between batches of writes of entries
- D) the batch size of entries given to the writers

To increase and sustain high throughput:
1. build a redis database with 2 shards/partitions so that each shard can have its own stream key
2. run 2 copies of this program providing the following args: (these will be writer instances)

Instance 1:
```  
--streamname X:FOR_PROCESSING{1} --resultsstreamname X:PROCESSED_EVENTS{1} --howmanyentries 200000 --writersleeptime 10 --howmanyworkers 0
```
Instance 2:
```
--streamname X:FOR_PROCESSING{2} --resultsstreamname X:PROCESSED_EVENTS{2} --howmanyentries 200000 --writersleeptime 10 --howmanyworkers 0
```
3. run 2 copies of this program providing the following args: (these will be the worker group instances)

Instance 1:
```
--howmanyworkers 20 --workersleeptime 10 --streamname X:FOR_PROCESSING{1} --resultskeyname X:PROCESSED_EVENTS{1}
```

Instance 2:
```
--howmanyworkers 20 --workersleeptime 10 --streamname X:FOR_PROCESSING{2} --resultskeyname X:PROCESSED_EVENTS{2}
```


### There are a ton of other flags/settings available -check the top of the main method in the class 'Main' to see what other things you can adjust.

## NOTE that the program will not exit on its own if it is running Worker Threads.