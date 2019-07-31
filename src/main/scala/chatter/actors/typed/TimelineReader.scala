package chatter
package actors
package typed

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.stream.{ActorAttributes, ActorMaterializer, ActorMaterializerSettings, Attributes, StreamRefAttributes}
import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel, StreamRefs}
import chatter.actors.typed.Replicator.v1.MessagePB
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope

import scala.concurrent.duration._

object TimelineReader {

  def apply(writer: ActorRef[WriteResponses], delay: FiniteDuration): Behavior[ReadReply] =
    Behaviors.setup { ctx ⇒
      val bs        = 1 << 5
      val sinkQueue = Sink.queue[MessagePB]().withAttributes(Attributes.inputBuffer(bs, bs))

      val st = ActorMaterializerSettings.create(ctx.system.toUntyped).withInputBuffer(bs, bs)
      implicit val mat = akka.stream.ActorMaterializer(
        ActorMaterializerSettings
          .create(ctx.system.toUntyped)
          .withStreamRefSettings(st.streamRefSettings.withSubscriptionTimeout(5.seconds).withBufferCapacity(bs))
      )(ctx.system.toUntyped)

      implicit val ec = mat.executionContext

      //doesn't work
      /*val settings = StreamRefAttributes
        .subscriptionTimeout(5.seconds)
        .and(akka.stream.Attributes.inputBuffer(bs, bs))*/

      val port = ctx.system.settings.config.getInt("akka.remote.netty.tcp.port")
      //val port = ctx.system.settings.config.getInt("akka.remote.artery.canonical.port")

      ctx.log.info(s"★ ★ ★  Reader $port ★ ★ ★")
      //if (port == 2550)
      ctx.scheduleOnce(delay, writer, AskForShards(ctx.self))

      //prepare a SinkRef
      def readPassive(
        chatId: Long,
        shards: Vector[Shard[ReplicatorProtocol]],
        replyTo: ActorRef[ReadReply]
      ): Unit = {
        val ind = (chatId % shards.size).toInt
        ctx.log.info("reader {} chat-{} -> ind:{}", port, chatId, ind)
        //Passive mode with StreamRefs.sinkRef. It says I'm ready to consume data. Send it to me"

        val (fSinkRef, outQueue) =
          StreamRefs
            .sinkRef[Array[Byte]]()
            //.addAttributes(settings)
            .takeWhile(_.size > 1) //needed if MergeHub and BroadcastHub are used
            .map(MessagePB.parseFrom)
            .toMat(sinkQueue)(Keep.both)
            .run()

        fSinkRef.foreach { sinkRef ⇒
          shards(ind) match {
            case LocalShard(_, ref) ⇒
              ref.tell(PassiveReadChatTimeline(chatId, sinkRef, replyTo))
            case RemoteShard(_, ref) ⇒
              ref.toUntyped ! ConsistentHashableEnvelope(PassiveReadChatTimeline(chatId, sinkRef, replyTo), chatId)
          }
        }
        drainQueue(chatId, shards, outQueue)
      }

      def drainQueue(
        chatId: Long,
        shards: Vector[Shard[ReplicatorProtocol]],
        q: SinkQueueWithCancel[MessagePB],
        cnt: Long = 0
      ): Unit =
        q.pull.map { r ⇒
          if (r.isDefined) drainQueue(chatId, shards, q, cnt + 1L)
          else {
            ctx.log.warning("done reader {} chat-{} -> size:{}", port, chatId, cnt)
            readPassive(chatId + 1L, shards, ctx.self)
          }
        }

      /*def readActive(
        chatId: Long,
        shards: Vector[Shard[ReplicatorProtocol]],
        replyTo: ActorRef[ReadReply]
      ): Behavior[ReadReply] = {
        val ind = (chatId % shards.size).toInt
        ctx.log.info("reader {} chat-{} -> ind:{}", port, chatId, ind)

        shards(ind) match {
          case LocalShard(_, ref) ⇒
            ref tell ReadChatTimeline(chatId, replyTo)
          case RemoteShard(_, ref) ⇒
            ref.toUntyped ! ConsistentHashableEnvelope(ReadChatTimeline(chatId, replyTo), chatId)
        }
        awaitSrc(chatId, shards)
      }

      def awaitSrc(chatId: Long, shards: Vector[Shard[ReplicatorProtocol]]): Behavior[ReadReply] =
        Behaviors.receiveMessage[ReadReply] {
          case RemoteChatTimelineSource(src) ⇒
            val f = src.map(MessagePB.parseFrom).runWith(Sink.seq)
            val r = scala.concurrent.Await.result(f, Duration.Inf)
            //Want to read one by one

            ctx.log.warning("chat-{} = {}", chatId, r.size)
            //.onComplete(_.foreach(seq ⇒ ctx.self.tell(RStreamingSuccess(seq))))(mat.executionContext)
            Behaviors.same
            readActive(chatId + 1L, shards, ctx.self)
          case RNotFound(name) ⇒
            ctx.log.warning(s"reader:$port  NotFoundChatTime: $name")
            Behaviors.stopped
          case RFailure(error) ⇒
            ctx.log.error(error)
            readActive(chatId + 1L, shards, ctx.self)
          case KnownShards(_) ⇒
            ctx.log.warning("Unexpected message: KnownShards")
            Behaviors.unhandled
        }*/

      Behaviors.receiveMessagePartial[ReadReply] {
        case KnownShards(shards) ⇒
          ctx.log.info("KnownShards: {}", shards.toString)
          readPassive(0L, shards, ctx.self)
          Behaviors.same

        //OR
        //readActive(0L, shards, ctx.self)
        case RNotFound(name) ⇒
          ctx.log.warning(s"reader:$port NotFoundChatTime: $name")
          Behaviors.same
      }
    }
}
