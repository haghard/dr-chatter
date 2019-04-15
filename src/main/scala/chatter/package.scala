import akka.cluster.ddata.Key
import chatter.actors.typed.ChatTimelineReplicator.ReplCommand
import chatter.crdt.ChatTimeline

package object chatter {

  trait Shard[T] {
    def name: String

    def ref: akka.actor.typed.ActorRef[T]
  }

  case class LocalShard(name: String, ref: akka.actor.typed.ActorRef[ReplCommand]) extends Shard[ReplCommand]

  case class RemoteShard(name: String, ref: akka.actor.typed.ActorRef[ReplCommand]) extends Shard[ReplCommand]

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