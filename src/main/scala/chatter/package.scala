import chatter.crdt.ChatTimeline
import chatter.actors.typed.ReplicatorProtocol
import akka.cluster.ddata.{Key, ORMap, ReplicatedData}

import scala.collection.immutable.SortedMap

package object chatter {

  trait Shard[T] {
    def name: String
    def ref: akka.actor.typed.ActorRef[T]
  }

  case class LocalShard(name: String, ref: akka.actor.typed.ActorRef[ReplicatorProtocol])
      extends Shard[ReplicatorProtocol]

  case class RemoteShard(name: String, ref: akka.actor.typed.ActorRef[ReplicatorProtocol])
      extends Shard[ReplicatorProtocol]

  case class Node(host: String, port: Int)

  case class Message(usrId: Long, cnt: String, when: Long, tz: String)

  case class ChatKey(chatName: String) extends Key[ChatTimeline](chatName)

  case class ChatBucket(bucketNbr: Long) extends Key[ORMap[String, ChatTimeline]](s"chat.bkt.$bucketNbr")

  object Implicits {
    val msgOrd: Ordering[Message] = (x: Message, y: Message) ⇒
      if (x.when < y.when) -1 else if (x.when > y.when) 1 else 0

    implicit val nodeOrd = new scala.Ordering[Node] {
      override def compare(a: Node, b: Node): Int =
        Ordering
          .fromLessThan[Node] { (x, y) ⇒
            if (x.host != y.host) x.host.compareTo(y.host) < 0
            else if (x.port != y.port) x.port < y.port
            else false
          }
          .compare(a, b)
    }
  }

  trait Partitioner[+T <: ReplicatedData] {
    type ReplicatedKey <: Key[T]

    //30 entities and 6 buckets.
    //Each bucket contains 5 entities which means instead of one ORMap at least (30/5) = 6 ORMaps will be used
    protected val maxNumber = 30L

    //(5l to maxNumber).by(5l).toArray
    protected val buckets: Array[Long] = Array(5L, 10L, 15L, 20L, 25L, maxNumber)

    protected val ring: SortedMap[Long, ChatBucket] =
      SortedMap(
        5L        → ChatBucket(0),
        10L       → ChatBucket(1),
        15L       → ChatBucket(2),
        20L       → ChatBucket(3),
        25L       → ChatBucket(4),
        maxNumber → ChatBucket(5)
      )

    protected val buckets0: Array[ChatBucket] =
      Array.tabulate(6)(i ⇒ ChatBucket(i))

    def keyForBucket(key: Long): ReplicatedKey
  }

  /** We want to split a global ORMap up in (30/5) = 6 top level ORMaps.
    * Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time
    * and you may get inconsistencies between related entries (of cause if you have related entries).
    * Separated top level entries cannot be updated atomically together.
    */
  trait ChatTimelineHashPartitioner extends Partitioner[ORMap[String, ChatTimeline]] {
    override type ReplicatedKey = ChatBucket

    def lookupFromRing(hash: Long): ChatBucket =
      (ring.valuesIteratorFrom(hash) ++ ring.valuesIteratorFrom(ring.firstKey)).next()

    override def keyForBucket(key: Long) = {
      import scala.collection.Searching._
      val bucketNum: Long = math.abs(key % maxNumber)
      //buckets0(math.abs(key % maxNumber).toInt)

      val i = buckets.search(bucketNum).insertionPoint
      //val i = math.abs(key.hashCode) % 10
      ChatBucket(i)
    }
  }
}
