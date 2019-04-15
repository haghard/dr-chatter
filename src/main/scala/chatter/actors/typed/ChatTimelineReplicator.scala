package chatter.actors.typed

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.ddata.typed.scaladsl.Replicator.{Command, Update, UpdateResponse}
import akka.cluster.ddata.typed.scaladsl.ReplicatorSettings
import chatter.actors.ChatTimelineWriter._
import chatter.crdt.ChatTimeline
import chatter.{ChatKey, Message, Node}
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed.scaladsl.adapter._

object ChatTimelineReplicator {

  val postfix = "-repl"

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

  def apply(system: ActorSystem, shardName: String, replyTo: ActorRef[RResponses]): Behavior[ReplicatorProtocol] = {
    Behaviors.setup[ReplicatorProtocol] { cnx ⇒

      val wc = WriteLocal //WriteTo(2, 3.seconds)
      //val rc = ReadLocal //ReadFrom(2, 3.seconds)
      val replicatorName = s"$shardName-repl"


      val cluster = Cluster(cnx.system.toUntyped)
      val address = cluster.selfUniqueAddress.address
      val node = Node(address.host.get, address.port.get)
      val config = replicatorConfig(replicatorName, shardName,
                                    classOf[akka.cluster.ddata.RocksDurableStore].getName)

      val akkaReplicator: ActorRef[Command] =
        system.spawn(akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(config)), replicatorName)

      cnx.log.info("★ ★ ★ Start typed-replicator {} backed by {}", replicatorName,
                                                                   config.getString("durable.store-actor-class"))

      val updateAdapter: ActorRef[UpdateResponse[ChatTimeline]] =
        cnx.messageAdapter({
          case akka.cluster.ddata.Replicator.UpdateSuccess(k @ ChatKey(_), _) ⇒
            WriteSuccess(k.chatName)
          case akka.cluster.ddata.Replicator.ModifyFailure(k @ ChatKey(_), _, cause, _) ⇒
            WriteFailure(k.chatName, cause.getMessage)
          case akka.cluster.ddata.Replicator.UpdateTimeout(k @ ChatKey(_), _) ⇒
            WriteTimeout(k.chatName)
          case akka.cluster.ddata.Replicator.StoreFailure(k @ ChatKey(_), _) ⇒
            WriteFailure(k.chatName, "StoreFailure")
        })

      Behaviors.receiveMessage[ReplicatorProtocol] {
        case msg: WriteChatTimeline ⇒
          val Key = ChatKey(msg.chatName)
          akkaReplicator ! Update(Key, ChatTimeline(), wc, updateAdapter, None) { tl ⇒
            tl + (Message(msg.authorId, msg.content, msg.when, msg.tz), node)
          }
          Behaviors.same
        case w: WriteSuccess ⇒
          replyTo ! w
          Behaviors.same
        case w: WriteFailure ⇒
          replyTo ! w
          Behaviors.same
        case w: WriteTimeout ⇒
          replyTo ! w
          Behaviors.same
      }
    }
  }
}
