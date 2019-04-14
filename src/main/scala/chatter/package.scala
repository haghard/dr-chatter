import akka.actor.ActorRef
import akka.cluster.ddata.Key
import chatter.crdt.ChatTimeline

package object chatter {

  trait Shard {
    def name: String

    def ref: ActorRef
  }

  case class LocalShard(name: String, ref: ActorRef) extends Shard

  case class RemoteShard(name: String, ref: ActorRef) extends Shard

  case class Node(host: String, port: Int)

  case class Message(authId: Long, cnt: String, when: Long, tz: String)

  case class ChatKey(chatName: String) extends Key[ChatTimeline](chatName)

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

}