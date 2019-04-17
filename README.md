## Eventually consistent, sharded, replicated chat timeline with akka (POC)

ChatTimeline crdt is backed by H2 or RocksDB

#How to run

```bash

sbt "runMain chatter.Runner"

```


#Typed actors
https://github.com/hseeberger/welcome-akka-typed/blob/master/src/main/scala/rocks/heikoseeberger/wat/typed/Transfer.scala
https://doc.akka.io/docs/akka/current/typed/routers.html
https://doc.akka.io/docs/akka/current/typed/distributed-data.html
https://github.com/johanandren/akka-typed-samples.git
https://github.com/hseeberger/whirlwind-tour-akka.git


#RocksDB
https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBColumnFamilySample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBSample.java
https://www.cockroachlabs.com/blog/consistency-model/
https://www.cockroachlabs.com/blog/cockroachdb-on-rocksd/
https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/


#Akka-cluster
https://scalac.io/split-brain-scenarios-with-akka-scala/


#Idea
https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0


#Things to address
Should not put each entry as a top level entry in Replicator, you'd rather should not have them all in one ORMap.
Split them over a reasonable number of ORMaps (hashing again). 
How it is done in Cluster Sharding: https://github.com/akka/akka/blob/master/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/Shard.scala#L594 