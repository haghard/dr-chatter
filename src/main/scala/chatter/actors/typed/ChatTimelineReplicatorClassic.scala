package chatter.actors.typed

import akka.actor.typed.{ ActorRef, Behavior, ExtensibleBehavior, PostStop, PreRestart, Signal, Terminated, TypedActorContext }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.Cluster
import akka.cluster.ddata.typed.scaladsl.{ DistributedData, ReplicatorSettings }
import akka.cluster.ddata.typed.scaladsl.Replicator.{ Command, Get, GetResponse, ReadLocal, Update, UpdateResponse, WriteLocal }
import chatter.{ ChatBucket, ChatHashPartitioner, Message, Node }
import chatter.actors.typed.ChatTimelineReplicator.replicatorConfig
import com.typesafe.config.{ Config, ConfigFactory }
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.ORMap
import chatter.crdt.ChatTimeline
import ChatTimelineReplicatorClassic._
import akka.cluster.ddata.Replicator.{ ReadFrom, WriteTo }

object ChatTimelineReplicatorClassic {

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

  def writeAdapter(ctx: ActorContext[ReplicatorCommand]): ActorRef[UpdateResponse[ORMap[String, ChatTimeline]]] =
    ctx.messageAdapter {
      case akka.cluster.ddata.Replicator.UpdateSuccess(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteSuccess(chatKey, replyTo)
      case akka.cluster.ddata.Replicator.ModifyFailure(k @ ChatBucket(_), _, cause, Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteFailure(chatKey, cause.getMessage, replyTo)
      case akka.cluster.ddata.Replicator.UpdateTimeout(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteTimeout(chatKey, replyTo)
      case akka.cluster.ddata.Replicator.StoreFailure(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteFailure(chatKey, "StoreFailure", replyTo)
      case other ⇒
        ctx.log.error("Unsupported message form replicator: {}", other)
        throw new Exception(s"Unsupported message form replicator: $other")
    }

  def readAdapter(ctx: ActorContext[ReplicatorCommand]): ActorRef[GetResponse[ORMap[String, ChatTimeline]]] =
    ctx.messageAdapter {
      case r @ akka.cluster.ddata.Replicator.GetSuccess(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
        val maybe = r.get[ORMap[String, ChatTimeline]](k).get(chatKey)
        maybe.fold[ReplicatorCommand](RNotFoundChatTimelineReply(chatKey, replyTo)) { r ⇒
          RChatTimelineReply(r.timeline, replyTo)
        }
      case akka.cluster.ddata.Replicator.GetFailure(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
        RGetFailureChatTimelineReply(s"GetFailure: ${chatKey}", replyTo)
      case akka.cluster.ddata.Replicator.NotFound(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
        RNotFoundChatTimelineReply(chatKey, replyTo)
      case other ⇒
        ctx.log.error("Unsupported message form replicator: {}", other)
        throw new Exception(s"Unsupported message form replicator: ${other}")
    }
}

//Implementation similar to a classic Actor
class ChatTimelineReplicatorClassic(ctx: ActorContext[Unit], shardName: String) extends ExtensibleBehavior[ReplicatorCommand]
  with ChatHashPartitioner {
  implicit val addr = DistributedData(ctx.system).selfUniqueAddress

  import scala.concurrent.duration._

  val wc = /*WriteLocal*/ WriteTo(2, 3.seconds)
  val rc = /*ReadLocal*/ ReadFrom(2, 3.seconds)

  val cluster = Cluster(ctx.system.toUntyped)
  val address = cluster.selfUniqueAddress.address
  val node = Node(address.host.get, address.port.get)
  val cnf = replicatorConfig(shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)

  val akkaReplicator: ActorRef[Command] = ctx.spawn(
    akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(cnf)), shardName + "-" + ChatTimelineReplicator.name)

  ctx.log.info("★ ★ ★ Start typed-replicator {} backed by {}", akkaReplicator.path, cnf.getString("durable.store-actor-class"))

  override def receive(
    ctx: TypedActorContext[ReplicatorCommand],
    msg: ReplicatorCommand): Behavior[ReplicatorCommand] =
    msg match {
      //********  writes *************/
      case msg: WriteMessage ⇒
        val BucketKey = keyForBucket(msg.chatId)
        val chatKey = s"chat.${msg.chatId}"
        akkaReplicator ! Update(BucketKey, ORMap.empty[String, ChatTimeline], wc, writeAdapter(ctx.asScala), Some((chatKey, msg.replyTo))) { bucket ⇒
          bucket.get(chatKey).fold(
            bucket :+ (chatKey -> ChatTimeline().+(Message(msg.authId, msg.content, msg.when, msg.tz), node))
          ) { es ⇒
              bucket :+ (chatKey, es + (Message(msg.authId, msg.content, msg.when, msg.tz), node))
            }
        }
        this
      case w: RWriteSuccess ⇒
        w.replyTo ! WSuccess(w.chatName)
        this
      case w: RWriteFailure ⇒
        w.replyTo ! WFailure(w.chatName, w.errorMsg)
        this
      case w: RWriteTimeout ⇒
        w.replyTo ! WTimeout(w.chatName)
        this

      //********  reads *************/
      case r: ReadChatTimeline ⇒
        val BucketKey = keyForBucket(r.chatId)
        val chatKey = s"chat.${r.chatId}"
        akkaReplicator ! Get(BucketKey, rc, readAdapter(ctx.asScala), Some((chatKey, r.replyTo)))
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

  override def receiveSignal(
    ctx: TypedActorContext[ReplicatorCommand],
    msg: Signal): Behavior[ReplicatorCommand] =
    msg match {
      case PreRestart ⇒
        this
      case PostStop ⇒
        this
      case Terminated(ref) ⇒
        this
    }
}
