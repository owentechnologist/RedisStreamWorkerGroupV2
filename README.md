## This program demonstrates writing and processing events using Redis Streams using Jedis 4.3.1

### A bunch has changed this August... I added the concept of a Topic
### Implemented as a List in Redis, it keeps track of all related streams

#### You can now setup a publisher that writes multiple streams to a topic
These are the arguments that trigger publishing:  
```
--topic someInterestingTopicA --howmanyentries 10000000 --maxstreamlength 1000000
``` 
#### Workers / ConsumerGroup Members now also use the --topic argument instead of a --streamname
#### The workers now move from Stream to Stream within a Topic: processing all entries
#### Starting with the oldest and continuing until the youngest

#### - A writer / Publisher writes X events/entries to the streams in a topic
#### - Some number of workers (belonging to a worker group) consume those entries and process them
#### - The processed entries are written to a separate stream or they are simply counted and the count is stored in a Redis String
These arguments establish if a stream is used to store processed entries or if they are counted:
```
--consumerresponseisastream false --resultskeyname topicA:workerGroup2:ResultCount
```

The default settings operate at around 100 ops/second and use:
- A **work to be done** stream name of "X:FOR_PROCESSING{1}"
- A **work completed** stream name of "X:PROCESSED_EVENTS{1}"
- One writer that writes 10000 entries in batches of 200 entries (with 50 millisecond pauses between each batch)
- A worker group of 2 workers that process the entries 1 at a time and sleep 50 milliseconds between each one

* To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host FIXME --port FIXME"
```

* To run the program much, much, much faster you can use the following command:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host FIXME --port FIXME --howmanyentries 100000 --howmanyworkers 20"
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