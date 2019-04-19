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
We should not put each entry as a top level entry in Replicator, you'd rather should not have them all in one ORMap.
Split them over a reasonable number of ORMaps (hashing again).

When a data entry is changed the full state of that entry is replicated to other nodes, i.e. when you update a map, the whole map is replicated. 
Therefore, instead of using one ORMap with 1000 elements it is more efficient to split that up in 10 top level ORMap entries with 100 elements each. 
Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time and you may see 
inconsistencies between related entries. Separate top level entries cannot be updated atomically together.
 
How it is done in Cluster Sharding: https://github.com/akka/akka/blob/master/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/Shard.scala#L594 