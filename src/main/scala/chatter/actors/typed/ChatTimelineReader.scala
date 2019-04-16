package chatter
package actors
package typed

import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.actor.typed.scaladsl.adapter._

import scala.concurrent.duration._

object ChatTimelineReader {

  def apply(writer: ActorRef[WriteResponses], delay: FiniteDuration): Behavior[ReadReply] = {
    Behaviors.setup { ctx ⇒
      val readTO = 50.millis

      ctx.log.info("★ ★ ★  Reader  ★ ★ ★")

      //ctx.ask(AskForShards(ctx.self))
      ctx.scheduleOnce(delay, writer, AskForShards(ctx.self))

      def read(chatId: Long, shards: Vector[Shard[ReplicatorCommand]], replyTo: ActorRef[ReadReply]): Behavior[ReadReply] = {
        val ind = (chatId % shards.size).toInt
        ctx.log.info("read chat-{} -> ind:{}", chatId, ind)
        shards(ind) match {
          case LocalShard(_, ref) ⇒
            ctx.scheduleOnce(readTO, ref, ReadChatTimeline(s"chat-$chatId", replyTo))
          case RemoteShard(_, ref) ⇒
            ctx.scheduleOnce(readTO, ref.toUntyped, ConsistentHashableEnvelope(ReadChatTimeline(s"chat-$chatId", replyTo), chatId))
        }
        await(chatId, shards)
      }

      def await(chatId: Long, shards: Vector[Shard[ReplicatorCommand]]): Behavior[ReadReply] =
        Behaviors.receiveMessage[ReadReply] {
          case RSuccess(h) ⇒
            ctx.log.warning("chat-{} = {}", chatId, h.size)
            read(chatId + 1l, shards, ctx.self)
          case RNotFound(name) ⇒
            ctx.log.warning("NotFoundChatTime: " + name)
            Behaviors.stopped
          case RFailure(error) ⇒
            ctx.log.error(error)
            read(chatId + 1l, shards, ctx.self)
          case KnownShards(_) ⇒
            ctx.log.warning("Unexpected message: KnownShards")
            Behaviors.unhandled
        }

      Behaviors.receiveMessagePartial[ReadReply] {
        case KnownShards(shards) ⇒
          ctx.log.info("KnownShards: {}", shards.toString)
          read(0l, shards, ctx.self)
      }
    }
  }
}
