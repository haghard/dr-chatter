package chatter
package actors

import java.util.UUID

import akka.cluster.Cluster
import com.typesafe.config.{ Config, ConfigFactory }
import akka.cluster.ddata.{ Replicator, ReplicatorSettings }
import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
import chatter.actors.ChatTimelineReplicator.{ LocalReadCtx, RemoteReadCtx, WriteCtx }
import chatter.actors.ChatTimelineWriter.{ ReadLocalChatTimeline, ReadRemoteChatTimeline, Write, WriteFailure, WriteSuccess, WriteTimeout }
import akka.cluster.ddata.Replicator.{ Get, GetFailure, GetSuccess, ModifyFailure, NotFound, ReadLocal, Update, UpdateSuccess, UpdateTimeout, WriteLocal }
import ChatTimelineReplicator._
import chatter.actors.ChatTimelineReader.{ GetFailureChatTimelineResponse, LocalChatTimelineResponse, NotFoundChatTimelineResponse, RemoteChatTimelineResponse }
import chatter.crdt.ChatTimeline

object ChatTimelineReplicator {

  sealed trait RequestCtx {
    def cId: String
  }

  case class WriteCtx(cId: String, start: Long, s: ActorRef) extends RequestCtx

  case class LocalReadCtx(cId: String, start: Long, s: ActorRef) extends RequestCtx

  case class RemoteReadCtx(cId: String, start: Long, s: ActorRef) extends RequestCtx

  def createConfig(replicatorName: String, shardName: String, clazz: String): Config = ConfigFactory.parseString(
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

  def props(system: ActorSystem, shardName: String) =
    Props(new ChatTimelineReplicator(system, shardName))
}

class ChatTimelineReplicator(system: ActorSystem, shardName: String) extends Actor with ActorLogging {
  val wc = WriteLocal //WriteTo(2, 3.seconds)
  val rc = ReadLocal //ReadFrom(2, 3.seconds)

  implicit val cluster = Cluster(context.system)

  val address = cluster.selfUniqueAddress.address
  val node = Node(address.host.get, address.port.get)

  val replicatorName = s"$shardName-repl"

  //classOf[akka.cluster.ddata.H2DurableStore].getName
  //classOf[akka.cluster.ddata.RocksDurableStore].getName

  val config = createConfig(replicatorName, shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)
  val dbClass = config.getString("durable.store-actor-class")
  val akkaReplicator = system.actorOf(Replicator.props(ReplicatorSettings(config)), replicatorName)

  override def preStart(): Unit =
    log.info("★ ★ ★ Start replicator {} backed by {}", replicatorName, dbClass)

  override def receive: Receive = write orElse read

  def write: Receive = {
    case msg: Write ⇒
      val Key = ChatKey(msg.chatName)
      val cId = UUID.randomUUID.toString
      val replyTo = sender()
      akkaReplicator ! Update(Key, ChatTimeline(), wc, Some(WriteCtx(cId, System.nanoTime, replyTo))) { tl ⇒
        tl + (Message(msg.authorId, msg.content, msg.when, msg.tz), node)
      }

    case UpdateSuccess(k @ ChatKey(_), Some(WriteCtx(cId, start, replyTo))) ⇒
      replyTo ! WriteSuccess(k.chatName)

    case ModifyFailure(k @ ChatKey(_), errorMessage, cause, Some(WriteCtx(_, startTs, replyTo))) ⇒
      replyTo ! WriteFailure(k.chatName, errorMessage)

    case UpdateTimeout(k @ ChatKey(_), Some(WriteCtx(cId, startTs, replyTo))) ⇒
      replyTo ! WriteTimeout(k.chatName)
  }

  def read: Receive = {
    /** local  read  ***/
    case ReadLocalChatTimeline(chatName) ⇒
      val cId = UUID.randomUUID.toString
      val Key = ChatKey(chatName)
      val replyTo = sender()
      akkaReplicator ! Get(Key, rc, Some(LocalReadCtx(cId, System.nanoTime, replyTo)))

    case r @ GetSuccess(k @ ChatKey(_), Some(LocalReadCtx(cId, startTs, replyTo))) ⇒
      replyTo ! LocalChatTimelineResponse(r.get[ChatTimeline](k).timeline)

    case GetFailure(k @ ChatKey(_), Some(LocalReadCtx(cId, startTs, replyTo))) ⇒
      log.error("Get request for {} could not be fulfill according to the given consistency level and timeout", k.chatName)
      replyTo ! GetFailureChatTimelineResponse(s"GetFailure: ${k.chatName}")

    case NotFound(k @ ChatKey(_), Some(LocalReadCtx(cId, startTs, replyTo))) ⇒
      log.error("NotFound by {}", k.chatName)
      replyTo ! NotFoundChatTimelineResponse(k.chatName)

    /** remote  read ***/
    case ReadRemoteChatTimeline(chatName) ⇒
      val cId = UUID.randomUUID.toString
      val Key = ChatKey(chatName)
      val replyTo = sender()
      akkaReplicator ! Get(Key, rc, Some(RemoteReadCtx(cId, System.nanoTime, replyTo)))

    case r @ GetSuccess(k @ ChatKey(_), Some(RemoteReadCtx(cId, start, replyTo))) ⇒
      val tl = r.get[ChatTimeline](k).timeline
      replyTo ! RemoteChatTimelineResponse(tl)

    //https://doc.akka.io/docs/akka/current/stream/stream-refs.html

    //stop working at 1800 msg per chat with rocksDB
    /*
    val bytes = tl
      .take(10) //first 10
      .map(m => MessagePB(m.when, m.tz, m.authorId,
      protobuf.ByteString.copyFrom(m.content.getBytes(Charsets.UTF_8))).toByteArray)

    import akka.pattern.pipe
    Source(bytes)//.grouped(50)
      .runWith(StreamRefs.sourceRef()
      .addAttributes(StreamRefAttributes.subscriptionTimeout(5.seconds)
      .and(Attributes.inputBuffer(2048, 2048))))
      .map(GetRemoteChatTimelineResponse(_))
      .pipeTo(replyTo)*/

    case GetFailure(k @ ChatKey(_), Some(RemoteReadCtx(cId, startTs, replyTo))) ⇒
      log.error("Get request for {} could not be fulfill according to the given consistency level and timeout", k.chatName)
      replyTo ! GetFailureChatTimelineResponse(s"GetFailure: ${k.chatName}")

    case NotFound(k @ ChatKey(_), Some(RemoteReadCtx(cId, startTs, replyTo))) ⇒
      log.error("NotFound by {}", k.chatName)
      replyTo ! NotFoundChatTimelineResponse(k.chatName)
  }
}