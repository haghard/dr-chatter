package chatter
package actors
package typed

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, Behavior }
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.{ ReadLocal, WriteLocal }
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Command, GetResponse, Update, UpdateResponse }
import akka.cluster.ddata.typed.scaladsl.ReplicatorSettings
import chatter.actors.ChatTimelineWriter._
import chatter.crdt.ChatTimeline
import com.typesafe.config.{ Config, ConfigFactory }
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.typed.scaladsl.Replicator._
import chatter.actors.typed.ChatTimelineReader.{ RFailure, RNotFound, RSuccess, ReadResponses }

object ChatTimelineReplicator {

  val postfix = "-repl"

  sealed trait ReplicatorOps
  case class WriteMessage(chatName: String, when: Long, tz: String, authId: Long, content: String) extends ReplicatorOps
  case class ReadChatTimeline(chatName: String, replyTo: ActorRef[ReadResponses]) extends ReplicatorOps

  //internal messages protocol
  private case class RWriteSuccessReq(chatName: String) extends ReplicatorOps
  private case class RWriteFailureReq(chatName: String, errorMsg: String) extends ReplicatorOps
  private case class RWriteTimeoutReq(chatName: String) extends ReplicatorOps

  private case class RChatTimelineResponse(history: Vector[Message], replyTo: ActorRef[ReadResponses]) extends ReplicatorOps
  private case class RNotFoundChatTimelineResponse(chatName: String, replyTo: ActorRef[ReadResponses]) extends ReplicatorOps
  private case class RGetFailureChatTimelineResponse(error: String, replyTo: ActorRef[ReadResponses]) extends ReplicatorOps

  def replicatorConfig(replicatorName: String, shardName: String, clazz: String): Config =
    ConfigFactory.parseString(
      s"""
         | name = $replicatorName
         |
         | role = $shardName
         |
         | gossip-interval = 2 s
         |
         | use-dispatcher = ""
         |
         | notify-subscribers-interval = 1 s
         |
         | max-delta-elements = 1000
         |
         | pruning-interval = 120 s
         |
         | max-pruning-dissemination = 300 s
         |
         | pruning-marker-time-to-live = 6 h
         |
         | serializer-cache-time-to-live = 10s
         |
         | delta-crdt {
         |   enabled = on
         |   max-delta-size = 1000
         | }
         |
         | durable {
         |
         |  keys = ["*"]
         |
         |  pruning-marker-time-to-live = 10 d
         |
         |  store-actor-class = $clazz
         |
         |  #store-actor-class = akka.cluster.ddata.RocksDurableStore
         |  #store-actor-class = akka.cluster.ddata.H2DurableStore
         |
         |  pinned-store {
         |    type = PinnedDispatcher
         |    executor = thread-pool-executor
         |  }
         |
         |  use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
         |
         |  rocks.dir = "ddata"
         |
         |  h2.dir = "ddata"
         |
         | }
        """.stripMargin)

  def apply(system: ActorSystem, shardName: String, replyTo: ActorRef[WriteResponses]): Behavior[ReplicatorOps] = {
    Behaviors.setup { cnx ⇒
      val replName = s"$shardName-repl"

      val wc = WriteLocal //WriteTo(2, 3.seconds)
      val rc = ReadLocal //ReadFrom(2, 3.seconds)

      val cluster = Cluster(cnx.system.toUntyped)
      val address = cluster.selfUniqueAddress.address
      val node = Node(address.host.get, address.port.get)
      val cnf = replicatorConfig(replName, shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)
      val akkaReplicator: ActorRef[Command] = system.spawn(
        akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(cnf)), replName)

      cnx.log.info("★ ★ ★ Start typed-replicator {} backed by {}", replName,
                                                                   cnf.getString("durable.store-actor-class"))

      val writeAdapter: ActorRef[UpdateResponse[ChatTimeline]] =
        cnx.messageAdapter {
          case akka.cluster.ddata.Replicator.UpdateSuccess(k @ ChatKey(_), _) ⇒
            RWriteSuccessReq(k.chatName) //WriteSuccess
          case akka.cluster.ddata.Replicator.ModifyFailure(k @ ChatKey(_), _, cause, _) ⇒
            RWriteFailureReq(k.chatName, cause.getMessage) //WriteFailure
          case akka.cluster.ddata.Replicator.UpdateTimeout(k @ ChatKey(_), _) ⇒
            RWriteTimeoutReq(k.chatName) //WriteTimeout
          case akka.cluster.ddata.Replicator.StoreFailure(k @ ChatKey(_), _) ⇒
            RWriteFailureReq(k.chatName, "StoreFailure") //WriteFailure
          case other ⇒
            cnx.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: $other")
        }

      val readAdapter: ActorRef[GetResponse[ChatTimeline]] =
        cnx.messageAdapter {
          case r @ akka.cluster.ddata.Replicator.GetSuccess(k @ ChatKey(_), Some(replyTo: ActorRef[ReadResponses] @unchecked)) ⇒
            RChatTimelineResponse(r.get[ChatTimeline](k).timeline, replyTo)
          case akka.cluster.ddata.Replicator.GetFailure(k @ ChatKey(_), Some(replyTo: ActorRef[ReadResponses] @unchecked)) ⇒
            RGetFailureChatTimelineResponse(s"GetFailure: ${k.chatName}", replyTo)
          case akka.cluster.ddata.Replicator.NotFound(k @ ChatKey(_), Some(replyTo: ActorRef[ReadResponses] @unchecked)) ⇒
            RNotFoundChatTimelineResponse(k.chatName, replyTo)
          case other ⇒
            cnx.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: ${other}")
        }

      Behaviors.receiveMessage[ReplicatorOps] {
        case msg: WriteMessage ⇒
          val Key = ChatKey(msg.chatName)
          akkaReplicator ! Update(Key, ChatTimeline(), wc, writeAdapter, None) { tl ⇒
            tl + (Message(msg.authId, msg.content, msg.when, msg.tz), node)
          }
          Behaviors.same
        case w: RWriteSuccessReq ⇒
          replyTo ! WSuccess(w.chatName)
          Behaviors.same
        case w: RWriteFailureReq ⇒
          replyTo ! WFailure(w.chatName, w.errorMsg)
          Behaviors.same
        case w: RWriteTimeoutReq ⇒
          replyTo ! WTimeout(w.chatName)
          Behaviors.same

        case r: ReadChatTimeline ⇒
          val Key = ChatKey(r.chatName)
          akkaReplicator ! Get(Key, rc, readAdapter, Some(r.replyTo))
          Behaviors.same
        case r: RChatTimelineResponse ⇒
          r.replyTo ! RSuccess(r.history)
          Behaviors.same
        case r: RGetFailureChatTimelineResponse ⇒
          r.replyTo ! RFailure(r.error)
          Behaviors.same
        case r: RNotFoundChatTimelineResponse ⇒
          r.replyTo ! RNotFound(r.chatName)
          Behaviors.same
      }
    }
  }
}
