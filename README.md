## Eventually consistent, sharded, replicated chat timeline with akka (POC)

ChatTimeline crdt is backed by RocksDB or H2

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


#Akka-cluster split brain
https://scalac.io/split-brain-scenarios-with-akka-scala/
https://doc.akka.io/docs/akka/current/distributed-data.html?language=scala

#Idea
https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0

You can use Cluster Sharding and DData with roles. So, let's say that you go with 10 roles, 10,000 entities in each role. You would then start Replicators on the nodes with corresponding nodes. You would also start Sharding on the nodes with corresponding roles. On a node that doesn't have the a role you would start a sharding proxy for such role.
When you want to send a message to an entity you first need to decide which role to use for that message. Can be simple hashCode modulo algorithm. Then you delegate the message to the corresponding Sharding region or proxy actor.
You have defined the Props for the entities and there you pass in the Replicator corresponding to the role that the entity belongs to, i.e. the entity takes the right Replicator ActorRef as constructor parameter.
If you don't need the strict guarantees of "only one entity" that Cluster Sharding provides, and prefer better availability in case of network partitions, you could use a consistent hashing group router instead of Cluster Sharding. You would have one router per role, and decide router similar as above. Then the entities (routees of the router) would have to subscribe to changes from DData to get notified of when a peer entity has changed something, since you can have more than one alive at the same time.


#Things to address
When a data entry is changed the full state of that entry is replicated to other nodes, i.e. when you update a map, the whole map is replicated. 
Therefore, instead of using one ORMap with 1000 elements it is more efficient to split that up in 10 top level ORMap entries with 100 elements each. 
Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time and you may see 
inconsistencies between related entries. Separate top level entries cannot be updated atomically together.
 
How it is done in Cluster Sharding: https://github.com/akka/akka/blob/master/akka-cluster-sharding/src/main/scala/akka/cluster/sharding/Shard.scala#L594 