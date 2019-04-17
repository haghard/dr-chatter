package chatter
package actors
package typed

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import akka.cluster.Cluster
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Command, GetResponse, UpdateResponse }
import akka.cluster.ddata.typed.scaladsl.ReplicatorSettings
import chatter.crdt.ChatTimeline
import com.typesafe.config.{ Config, ConfigFactory }
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.Replicator.{ ReadFrom, WriteTo }
import akka.cluster.ddata.typed.scaladsl.Replicator._

object ChatTimelineReplicator {

  val name = "replicator"

  def replicatorConfig(shardName: String, clazz: String): Config =
    ConfigFactory.parseString(
      s"""
         | role = $shardName
         | gossip-interval = 2 s
         | use-dispatcher = ""
         | notify-subscribers-interval = 1 s
         | max-delta-elements = 1000
         | pruning-interval = 120 s
         | max-pruning-dissemination = 300 s
         | pruning-marker-time-to-live = 6 h
         | serializer-cache-time-to-live = 10s
         | delta-crdt {
         |   enabled = on
         |   max-delta-size = 1000
         | }
         |
         | durable {
         |  keys = ["*"]
         |  pruning-marker-time-to-live = 10 d
         |  store-actor-class = $clazz
         |  #akka.cluster.ddata.RocksDurableStore
         |  #akka.cluster.ddata.H2DurableStore
         |  #akka.cluster.ddata.LmdbDurableStore
         |
         |  pinned-store {
         |    type = PinnedDispatcher
         |    executor = thread-pool-executor
         |  }
         |  
         |  use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
         |  rocks.dir = "ddata"
         |  h2.dir = "ddata"
         | }
        """.stripMargin)

  def apply(shardName: String): Behavior[ReplicatorCommand] = {
    Behaviors.setup { cxt ⇒
      import scala.concurrent.duration._
      val wc = WriteLocal // WriteTo(2, 3.seconds)
      val rc = ReadLocal //ReadFrom(2, 3.seconds)

      val cluster = Cluster(cxt.system.toUntyped)
      val address = cluster.selfUniqueAddress.address
      val node = Node(address.host.get, address.port.get)
      val cnf = replicatorConfig(shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)
      val akkaReplicator: ActorRef[Command] = cxt.spawn(
        akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(cnf)), ChatTimelineReplicator.name)

      cxt.log.info("★ ★ ★ Start typed-replicator {} backed by {}", akkaReplicator.path, cnf.getString("durable.store-actor-class"))

      val writeAdapter: ActorRef[UpdateResponse[ChatTimeline]] =
        cxt.messageAdapter {
          case akka.cluster.ddata.Replicator.UpdateSuccess(k @ ChatKey(_), Some(replyTo: ActorRef[WriteResponses] @unchecked)) ⇒
            RWriteSuccess(k.chatName, replyTo)
          case akka.cluster.ddata.Replicator.ModifyFailure(k @ ChatKey(_), _, cause, Some(replyTo: ActorRef[WriteResponses] @unchecked)) ⇒
            RWriteFailure(k.chatName, cause.getMessage, replyTo)
          case akka.cluster.ddata.Replicator.UpdateTimeout(k @ ChatKey(_), Some(replyTo: ActorRef[WriteResponses] @unchecked)) ⇒
            RWriteTimeout(k.chatName, replyTo)
          case akka.cluster.ddata.Replicator.StoreFailure(k @ ChatKey(_), Some(replyTo: ActorRef[WriteResponses] @unchecked)) ⇒
            RWriteFailure(k.chatName, "StoreFailure", replyTo)
          case other ⇒
            cxt.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: $other")
        }

      val readAdapter: ActorRef[GetResponse[ChatTimeline]] =
        cxt.messageAdapter {
          case r @ akka.cluster.ddata.Replicator.GetSuccess(k @ ChatKey(_), Some(replyTo: ActorRef[ReadReply] @unchecked)) ⇒
            RChatTimelineReply(r.get[ChatTimeline](k).timeline, replyTo)
          case akka.cluster.ddata.Replicator.GetFailure(k @ ChatKey(_), Some(replyTo: ActorRef[ReadReply] @unchecked)) ⇒
            RGetFailureChatTimelineReply(s"GetFailure: ${k.chatName}", replyTo)
          case akka.cluster.ddata.Replicator.NotFound(k @ ChatKey(_), Some(replyTo: ActorRef[ReadReply] @unchecked)) ⇒
            RNotFoundChatTimelineReply(k.chatName, replyTo)
          case other ⇒
            cxt.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: ${other}")
        }

      val write = Behaviors.receiveMessagePartial[ReplicatorCommand] {
        case msg: WriteMessage ⇒
          val Key = ChatKey(msg.chatName)
          akkaReplicator ! Update(Key, ChatTimeline(), wc, writeAdapter, Some(msg.replyTo)) { tl ⇒
            tl + (Message(msg.authId, msg.content, msg.when, msg.tz), node)
          }
          Behaviors.same
        case w: RWriteSuccess ⇒
          w.replyTo ! WSuccess(w.chatName)
          Behaviors.same
        case w: RWriteFailure ⇒
          w.replyTo ! WFailure(w.chatName, w.errorMsg)
          Behaviors.same
        case w: RWriteTimeout ⇒
          w.replyTo ! WTimeout(w.chatName)
          Behaviors.same
      }

      val read = Behaviors.receiveMessagePartial[ReplicatorCommand] {
        case r: ReadChatTimeline ⇒
          val Key = ChatKey(r.chatName)
          akkaReplicator ! Get(Key, rc, readAdapter, Some(r.replyTo))
          Behaviors.same
        case r: RChatTimelineReply ⇒
          r.replyTo ! RSuccess(r.history)
          Behaviors.same
        case r: RGetFailureChatTimelineReply ⇒
          r.replyTo ! RFailure(r.error)
          Behaviors.same
        case r: RNotFoundChatTimelineReply ⇒
          r.replyTo ! RNotFound(r.chatName)
          Behaviors.same
      }

      write orElse read
    }
  }
}
