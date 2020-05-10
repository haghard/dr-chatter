package chatter.actors.typed

import java.io.{File, FileOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import akka.actor.typed.{ActorRef, Behavior, ExtensibleBehavior, PostStop, PreRestart, Signal, Terminated, TypedActorContext}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.Cluster
import akka.cluster.ddata.typed.scaladsl.{DistributedData, ReplicatorSettings}
import akka.cluster.ddata.typed.scaladsl.Replicator.{Command, Get, GetResponse, ReadLocal, Update, UpdateResponse, WriteLocal}
import chatter.{ChatBucket, ChatTimelineHashPartitioner, Message, Node}
import chatter.actors.typed.ChatTimelineReplicator.replicatorConfig
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.ddata.ORMap
import chatter.crdt.ChatTimeline
import ChatTimelineReplicatorClassic._
import akka.cluster.ddata
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.Replicator.{ReadFrom, WriteTo}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, SinkQueueWithCancel, Source, StreamRefs}
import akka.stream.{ActorMaterializerSettings, Attributes, OverflowStrategy, SinkRef, StreamRefAttributes}
import chatter.actors.typed.Replicator.v1.MessagePB
import org.HdrHistogram.Histogram

import scala.concurrent.duration._

object ChatTimelineReplicatorClassic {

  val name = "replicator"

  def replicatorConfig(shardName: String, clazz: String): Config =
    ConfigFactory.parseString(
      s"""
         | role = $shardName
         | gossip-interval = 3 s
         | use-dispatcher = ""
         | notify-subscribers-interval = 3 s
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

  def writeAdapter(ctx: ActorContext[ReplicatorProtocol]): ActorRef[UpdateResponse[ORMap[String, ChatTimeline]]] =
    ctx.messageAdapter {
      case ddata.Replicator
            .UpdateSuccess(
            k @ ChatBucket(_),
            Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked, start: Long))
            ) ⇒
        RWriteSuccess(chatKey, replyTo, start)
      case ddata.Replicator.ModifyFailure(
          k @ ChatBucket(_),
          _,
          cause,
          Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))
          ) ⇒
        RWriteFailure(chatKey, cause.getMessage, replyTo)
      case ddata.Replicator
            .UpdateTimeout(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteTimeout(chatKey, replyTo)
      case ddata.Replicator
            .StoreFailure(k @ ChatBucket(_), Some((chatKey: String, replyTo: ActorRef[WriteResponses] @unchecked))) ⇒
        RWriteFailure(chatKey, "StoreFailure", replyTo)
      case other ⇒
        ctx.log.error("Unsupported message form replicator: {}", other)
        throw new Exception(s"Unsupported message form replicator: $other")
    }

  def readAdapter(ctx: ActorContext[ReplicatorProtocol]): ActorRef[GetResponse[ORMap[String, ChatTimeline]]] =
    ctx.messageAdapter {
      case r @ ddata.Replicator
            .GetSuccess(
              k @ ChatBucket(_),
              Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked, start: Long))
            ) ⇒
        r.get[ORMap[String, ChatTimeline]](k)
          .get(chatKey)
          .fold[ReplicatorProtocol](RNotFoundChatTimelineReply(chatKey, replyTo))(RChatTimelineReply(_, replyTo, start))

      case r @ ddata.Replicator.GetSuccess(
            k @ ChatBucket(_),
            Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked, sinkRef: SinkRef[Array[Byte]], start: Long))
          ) ⇒
        r.get[ORMap[String, ChatTimeline]](k)
          .get(chatKey)
          .fold[ReplicatorProtocol](RNotFoundChatTimelineReply(chatKey, replyTo))(
            RChatTimelineReplySink(_, sinkRef, replyTo, start)
          )

      case ddata.Replicator
            .GetFailure(
            k @ ChatBucket(_),
            Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked, sinkRef, start: Long))
            ) ⇒
        RGetFailureChatTimelineReply(s"GetFailure: ${chatKey}", replyTo)
      case ddata.Replicator
            .NotFound(
            k @ ChatBucket(_),
            Some((chatKey: String, replyTo: ActorRef[ReadReply] @unchecked, sinkRef, start: Long))
            ) ⇒
        RNotFoundChatTimelineReply(chatKey, replyTo)
      case other ⇒
        //ctx.log.error("Unsupported message form replicator: {}", other)
        throw new Exception(s"Unsupported message form replicator $other")
    }
}

//Implementation similar to a classic Actor
class ChatTimelineReplicatorClassic(ctx: ActorContext[Unit], shardName: String)
    extends ExtensibleBehavior[ReplicatorProtocol]
    with ChatTimelineHashPartitioner {
  implicit val addr = DistributedData(ctx.system).selfUniqueAddress

  val bs = 1 << 5
  val to = 5.seconds

  val wc = WriteLocal //WriteTo(2, 3.seconds)
  val rc = /*ReadLocal*/ ReadFrom(2, to)

  val cluster = Cluster(ctx.system.toClassic)
  val address = cluster.selfUniqueAddress.address
  val node    = Node(address.host.get, address.port.get)
  val cnf     = replicatorConfig(shardName, classOf[akka.cluster.ddata.RocksDurableStore].getName)

  val akkaReplicator: ActorRef[Command] = ctx.spawn(
    akka.cluster.ddata.typed.scaladsl.Replicator.behavior(ReplicatorSettings(cnf)),
    shardName + "-" + ChatTimelineReplicator.name
  )

  ctx.log.info(
    "★ ★ ★ Start typed-replicator {} backed by {}",
    akkaReplicator.path,
    cnf.getString("durable.store-actor-class")
  )

  val st = ActorMaterializerSettings.create(ctx.system.toClassic).withInputBuffer(bs, bs)
  implicit val mat = akka.stream.ActorMaterializer(
    ActorMaterializerSettings
      .create(ctx.system.toClassic)
      .withInputBuffer(bs, bs)
      .withStreamRefSettings(st.streamRefSettings.withSubscriptionTimeout(to).withBufferCapacity(bs))
  )(ctx.system.toClassic)

  /*implicit val mat = akka.stream.ActorMaterializer(
    ActorMaterializerSettings
      .create(ctx.system.toClassic)
      .withInputBuffer(bs, bs)
    //.withDispatcher("")
  )(ctx.system.toClassic)*/

  implicit val ec = mat.executionContext

  val atts = StreamRefAttributes
    .subscriptionTimeout(to)
    .and(akka.stream.Attributes.inputBuffer(bs, bs))

  val h = new Histogram(3600000000000L, 3)

  //BroadcastHub.sink
  /*val (sinkHub, srcHub) =
    MergeHub
      .source[Array[Byte]](bs)
      .toMat(BroadcastHub.sink(1))(Keep.both)
      .run()(mat)*/

  /*val (sinkHub0, sinkQ) =
    MergeHub
      .source[Array[Byte]](bs)
      .toMat(Sink.queue().withAttributes(Attributes.inputBuffer(bs, bs)))(Keep.both)
      .run()(mat)*/

  /*Source.queue(bs, OverflowStrategy.backpressure)
      .toMat(Sink.queue[Array[Byte]])(Keep.left)
      .run()*/

  /*
  def drain(
    q: SinkQueueWithCancel[Array[Byte]],
    limit: Int,
    acc: List[Array[Byte]] = Nil
  ): Future[List[Array[Byte]]] =
    q.pull().flatMap { r ⇒
      if (limit > 0) drain(q, limit - 1, r.get :: acc)
      else Future.successful(acc)
    }
   */

  //https://doc.akka.io/docs/akka/current/stream/stream-refs.html
  override def receive(
    ctx: TypedActorContext[ReplicatorProtocol],
    msg: ReplicatorProtocol
  ): Behavior[ReplicatorProtocol] =
    msg match {
      //********  writes *************/
      case msg: WriteMessage ⇒
        val BucketKey = keyForBucket(msg.chatId)
        val chatKey   = s"chat.${msg.chatId}"
        akkaReplicator ! Update(
          BucketKey,
          ORMap.empty[String, ChatTimeline],
          wc,
          writeAdapter(ctx.asScala),
          Some((chatKey, msg.replyTo, System.nanoTime))
        ) { bucket ⇒
          bucket
            .get(chatKey)
            .fold(
              bucket :+ (chatKey → ChatTimeline().+(Message(msg.userId, msg.content, msg.when, msg.tz), node))
            )(es ⇒ bucket :+ (chatKey, es + (Message(msg.userId, msg.content, msg.when, msg.tz), node)))
        }
        this
      case w: RWriteSuccess ⇒
        //h.recordValue(System.nanoTime - w.start)
        w.replyTo ! WSuccess(w.chatName)
        this
      case w: RWriteFailure ⇒
        w.replyTo ! WFailure(w.chatName, w.errorMsg)
        this
      case w: RWriteTimeout ⇒
        w.replyTo ! WTimeout(w.chatName)
        this

      //********  passive reads *************/
      case r: PassiveReadChatTimeline ⇒
        val BucketKey = keyForBucket(r.chatId)
        val chatKey   = s"chat.${r.chatId}"
        akkaReplicator tell Get(
          BucketKey,
          rc,
          readAdapter(ctx.asScala),
          Some((chatKey, r.replyTo, r.sinkRef, System.nanoTime))
        )
        Behaviors.same

      case r: RChatTimelineReplySink ⇒
        val batch = r.tl.timeline
        val src = Source
            .fromIterator(() ⇒ batch.iterator)
            .map(m ⇒
              MessagePB(
                m.usrId,
                com.google.protobuf.ByteString.copyFrom(m.cnt.getBytes(StandardCharsets.UTF_8)),
                m.when,
                m.tz
              ).toByteArray
            ) ++ Source.single(Array[Byte](0)) //to be able to stop gracefully

        //src.runWith(sinkHub0)
        /*Source
          .fromFuture(drain(sinkQ, batch.size))
          .mapConcat(identity)
          .runWith(r.sinkRef)*/

        /*
        src.runWith(sinkHub)
        srcHub.runWith(r.sinkRef)
         */

        src.runWith(r.sinkRef) //stream the result back
        h.recordValue(System.nanoTime - r.start)
        Behaviors.same

      //********  active reads *************/
      case r: ReadChatTimeline ⇒
        val BucketKey = keyForBucket(r.chatId)
        val chatKey   = s"chat.${r.chatId}"
        akkaReplicator tell Get(BucketKey, rc, readAdapter(ctx.asScala), Some((chatKey, r.replyTo, System.nanoTime)))
        Behaviors.same

      case r: RChatTimelineReply ⇒
        //Source(r.tl.timeline)
        val srcRef =
          Source
            .fromIterator(() ⇒ r.tl.timeline.iterator)
            .map(m ⇒
              MessagePB(
                m.usrId,
                com.google.protobuf.ByteString.copyFrom(m.cnt.getBytes(StandardCharsets.UTF_8)),
                m.when,
                m.tz
              ).toByteArray
            )
            //.take(bs)
            .toMat(StreamRefs.sourceRef[Array[Byte]].addAttributes(atts))(Keep.right)
            .run()

        r.replyTo.tell(RemoteChatTimelineSource(srcRef))

        /*Source // (r.tl.timeline.take(bs))
          .fromIterator(() ⇒ r.tl.timeline.iterator)
          .take(bs)
          .map(
            m ⇒
              MessagePB(
                m.authId,
                com.google.protobuf.ByteString.copyFrom(m.cnt.getBytes(StandardCharsets.UTF_8)),
                m.when,
                m.tz
              ).toByteArray
          )
          .runWith(StreamRefs.sourceRef().addAttributes(atts))
          .map(RemoteChatTimelineResponse(_))(ec)
          .pipeTo(r.replyTo.toUntyped)
         */

        //https://doc.akka.io/docs/akka/current/typed/stream.html#actor-source
        //ActorSink.actorRefWithAck[MessagePB](ref = r.replyTo, ...)

        //r.replyTo ! RSuccess(r.tl)
        Behaviors.same
      case r: RGetFailureChatTimelineReply ⇒
        r.replyTo ! RFailure(r.error)
        Behaviors.same
      case r: RNotFoundChatTimelineReply ⇒
        val f   = new File("./histograms/" + address.port.get + ".txt")
        val out = new FileOutputStream(f)
        h.outputPercentileDistribution(new PrintStream(out), 1000000.0) //in millis

        //Thread.sleep(500)

        r.replyTo ! RNotFound(r.chatName)
        Behaviors.same
    }

  override def receiveSignal(ctx: TypedActorContext[ReplicatorProtocol], msg: Signal): Behavior[ReplicatorProtocol] =
    msg match {
      case PreRestart ⇒
        ctx.asScala.log.error("!!!!!! PreRestart")
        this
      case PostStop ⇒
        ctx.asScala.log.error("!!!!!! PostStop")
        this
      case Terminated(ref) ⇒
        ctx.asScala.log.error(s"!!!!!! Terminated(${ref})")
        this
    }
}
