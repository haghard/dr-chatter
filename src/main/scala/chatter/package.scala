import chatter.crdt.ChatTimeline
import chatter.actors.typed.ReplicatorCommand
import akka.cluster.ddata.{ Key, ORMap, ReplicatedData }

package object chatter {

  trait Shard[T] {
    def name: String
    def ref: akka.actor.typed.ActorRef[T]
  }

  case class LocalShard(name: String, ref: akka.actor.typed.ActorRef[ReplicatorCommand]) extends Shard[ReplicatorCommand]

  case class RemoteShard(name: String, ref: akka.actor.typed.ActorRef[ReplicatorCommand]) extends Shard[ReplicatorCommand]

  case class Node(host: String, port: Int)

  case class Message(authId: Long, cnt: String, when: Long, tz: String)

  case class ChatKey(chatName: String) extends Key[ChatTimeline](chatName)

  case class ChatBucket(bucketNbr: Long) extends Key[ORMap[String, ChatTimeline]](s"chat.bkt.${bucketNbr}")

  object Implicits {
    val msgOrd: Ordering[Message] = (x: Message, y: Message) ⇒
      if (x.when < y.when) -1 else if (x.when > y.when) 1 else 0

    implicit val nodeOrd = new scala.Ordering[Node] {
      override def compare(a: Node, b: Node) =
        Ordering.fromLessThan[Node] { (x, y) ⇒
          if (x.host != y.host) x.host.compareTo(y.host) < 0
          else if (x.port != y.port) x.port < y.port
          else false
        }.compare(a, b)
    }
  }

  trait Partitioner[+T <: ReplicatedData] {
    type ReplicatedKey <: Key[T]

    //30 entities and 6 buckets.
    //Each bucket contains 5 entities which means instead of one ORMap at least (30/5) = 6 ORMaps will be used
    protected val maxNumber = 30l
    protected val buckets = Array(5l, 10l, 15l, 20l, 25, maxNumber)

    def keyForBucket(key: Long): ReplicatedKey
  }

  /**
   * We want to split a global ORMap up in (30/5) = 6 top level ORMaps.
   * Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time
   * and you may get inconsistencies between related entries (of cause if you have related entries).
   * Separated top level entries cannot be updated atomically together.
   */
  trait ChatHashPartitioner extends Partitioner[ORMap[String, ChatTimeline]] {
    override type ReplicatedKey = ChatBucket

    override def keyForBucket(key: Long) = {
      import scala.collection.Searching._
      val index = math.abs(key % maxNumber)
      ChatBucket(buckets.search(index).insertionPoint)
    }
  }
}