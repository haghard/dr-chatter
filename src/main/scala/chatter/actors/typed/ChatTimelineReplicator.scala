package chatter
package actors
package typed

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.Cluster
import akka.cluster.ddata.typed.scaladsl.Replicator.{Command, GetResponse, UpdateResponse}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, ReplicatorSettings}
import chatter.crdt.ChatTimeline
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.{Key, ORMap, ORMapKey, SelfUniqueAddress}
import akka.cluster.ddata.Replicator.{ReadFrom, WriteTo}
import akka.cluster.ddata.typed.scaladsl.Replicator._
import scala.concurrent.duration._

object ChatTimelineReplicator {

  val name = "replicator"

  object TopLevelKeysPartitioner extends ChatTimelineHashPartitioner

  /*def dataKey(entryKey: String): Key[ORMap[String, ChatTimeline]] =
    ORMapKey.create[String, ChatTimeline]("chat.bkt." + math.abs(entryKey.hashCode) % 100)*/

  // https://doc.akka.io/docs/akka/current/typed/distributed-data.html#using-the-replicator
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
        """.stripMargin
    )

  def apply(shardName: String): Behavior[ReplicatorProtocol] =
    Behaviors.setup { ctx ⇒
      val wc = WriteLocal // WriteTo(2, 3.seconds)
      val rc = ReadLocal  //ReadFrom(2, 3.seconds)

      implicit val addr = DistributedData(ctx.system).selfUniqueAddress

      val cluster = Cluster(ctx.system.toUntyped)
      val address = cluster.selfUniqueAddress.address
      val node    = Node(address.host.get, address.port.get)
      val cnf     = replicatorConfig(shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)
      val akkaReplicator: ActorRef[Command] = ctx.spawn(
        akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(cnf)),
        ChatTimelineReplicator.name
      )

      ctx.log.info(
        "★ ★ ★ Start typed-replicator {} backed by {}",
        akkaReplicator.path,
        cnf.getString("durable.store-actor-class")
      )

      val writeAdapter: ActorRef[UpdateResponse[ORMap[String, ChatTimeline]]] =
        ctx.messageAdapter {
          case akka.cluster.ddata.Replicator.UpdateSuccess(
              k @ ChatBucket(_),
              Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))
              ) ⇒
            RWriteSuccess(chatKey, replyTo)
          case akka.cluster.ddata.Replicator.ModifyFailure(
              k @ ChatBucket(_),
              _,
              cause,
              Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))
              ) ⇒
            RWriteFailure(chatKey, cause.getMessage, replyTo)
          case akka.cluster.ddata.Replicator.UpdateTimeout(
              k @ ChatBucket(_),
              Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))
              ) ⇒
            RWriteTimeout(chatKey, replyTo)
          case akka.cluster.ddata.Replicator.StoreFailure(
              k @ ChatBucket(_),
              Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))
              ) ⇒
            RWriteFailure(chatKey, "StoreFailure", replyTo)
          case other ⇒
            ctx.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: $other")
        }

      val readAdapter: ActorRef[GetResponse[ORMap[String, ChatTimeline]]] =
        ctx.messageAdapter {
          case r @ akka.cluster.ddata.Replicator
                .GetSuccess(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
            val maybe = r.get[ORMap[String, ChatTimeline]](k).get(chatKey)
            maybe.fold[ReplicatorProtocol](RNotFoundChatTimelineReply(chatKey, replyTo))(RChatTimelineReply(_, replyTo))
          case akka.cluster.ddata.Replicator
                .GetFailure(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
            RGetFailureChatTimelineReply(s"GetFailure: ${chatKey}", replyTo)
          case akka.cluster.ddata.Replicator
                .NotFound(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked))) ⇒
            RNotFoundChatTimelineReply(chatKey, replyTo)
          case other ⇒
            ctx.log.error("Unsupported message form replicator: {}", other)
            throw new Exception(s"Unsupported message form replicator: ${other}")
        }

      Behaviors.receiveMessage[ReplicatorProtocol] {
        //write
        case msg: WriteMessage ⇒
          //val Key = ChatKey(msg.chatName)
          val BucketKey = TopLevelKeysPartitioner.keyForBucket(msg.chatId)
          val chatKey   = s"chat.${msg.chatId}"

          akkaReplicator ! Update(
            BucketKey,
            ORMap.empty[String, ChatTimeline],
            wc,
            writeAdapter,
            Some((chatKey, msg.replyTo))
          ) { bucket ⇒
            bucket
              .get(chatKey)
              .fold(
                bucket :+ (chatKey → ChatTimeline().+(Message(msg.userId, msg.content, msg.when, msg.tz), node))
              ) { es ⇒
                bucket :+ (chatKey, es + (Message(msg.userId, msg.content, msg.when, msg.tz), node))
              }
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

        //read
        case r: ReadChatTimeline ⇒
          val BucketKey = TopLevelKeysPartitioner.keyForBucket(r.chatId)
          val chatKey   = s"chat.${r.chatId}"
          akkaReplicator ! Get(BucketKey, rc, readAdapter, Some((chatKey, r.replyTo)))
          Behaviors.same
        case r: RChatTimelineReply ⇒
          r.replyTo ! RSuccess(r.tl)
          Behaviors.same
        case r: RGetFailureChatTimelineReply ⇒
          r.replyTo ! RFailure(r.error)
          Behaviors.same
        case r: RNotFoundChatTimelineReply ⇒
          r.replyTo ! RNotFound(r.chatName)
          Behaviors.same
      }
    }
}
