package chatter
package actors
package typed

import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import chatter.actors.ChatTimelineWriter.{ AskForShards, WriteResponses }
import chatter.actors.typed.ChatTimelineReplicator.{ ReadChatTimeline, ReplicatorOps }
import akka.actor.typed.scaladsl.adapter._
import scala.concurrent.duration._

object ChatTimelineReader {

  sealed trait ReadResponses
  case class KnownShards(shards: Vector[Shard[ReplicatorOps]]) extends ReadResponses
  case class RSuccess(history: Vector[Message]) extends ReadResponses
  case class RNotFound(chatName: String) extends ReadResponses
  case class RFailure(chatName: String) extends ReadResponses

  def apply(writer: ActorRef[WriteResponses]): Behavior[ReadResponses] = {
    Behaviors.setup { ctx ⇒
      val readTO = 50.millis

      ctx.log.info("★ ★ ★  Spawned Reader")
      writer ! AskForShards(ctx.self)

      def read(chatId: Long, shards: Vector[Shard[ReplicatorOps]], replyTo: ActorRef[ReadResponses]): Behavior[ReadResponses] = {
        val ind = (chatId % shards.size).toInt
        ctx.log.info("read chat-{} -> ind:{}", chatId, ind)
        shards(ind) match {
          case LocalShard(_, ref) ⇒
            ctx.scheduleOnce(readTO, ref, ReadChatTimeline(s"chat-$chatId", replyTo))
          case RemoteShard(_, ref) ⇒
            ctx.scheduleOnce(readTO, ref.toUntyped, ConsistentHashableEnvelope(ReadChatTimeline(s"chat-$chatId", replyTo), chatId))
        }
        active(chatId, shards)
      }

      def active(chatId: Long, shards: Vector[Shard[ReplicatorOps]]): Behavior[ReadResponses] =
        Behaviors.receiveMessage[ReadResponses] {
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

      Behaviors.receiveMessage[ReadResponses] {
        case KnownShards(shards) ⇒
          ctx.log.info("KnownShards: {}", shards.toString)
          read(0l, shards, ctx.self)
        case other ⇒
          ctx.log.warning("Unexpected message: " + other)
          Behaviors.unhandled
      }
    }
  }
}