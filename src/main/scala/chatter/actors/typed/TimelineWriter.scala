package chatter
package actors
package typed

import java.time.ZoneId
import java.util.concurrent.ThreadLocalRandom

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import scala.util.Random

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope

object TimelineWriter {

  def apply(shards: Vector[Shard[ReplicatorProtocol]], ids: Seq[Long]): Behavior[WriteResponses] =
    Behaviors.setup { ctx ⇒
      val writeTO = 100.millis
      val rnd     = new Random(System.currentTimeMillis)

      Behaviors.withTimers[WriteResponses] { timers ⇒
        timers.startSingleTimer(ctx.self.path.toString, StartWriting, 2.second)

        def write(n: Long, shards: Vector[Shard[ReplicatorProtocol]]): Behavior[WriteResponses] = {
          val chatId = ids(ThreadLocalRandom.current.nextInt(ids.size))
          val userId = ThreadLocalRandom.current.nextLong(0L, 10L)

          //s"chat-$chatId"
          val msg = WriteMessage(
            chatId,
            System.currentTimeMillis,
            ZoneId.systemDefault.getId,
            userId,
            rnd.nextString(1024 * 1),
            ctx.self
          )

          shards(chatId.toInt % shards.size) match {
            case LocalShard(_, ref) ⇒
              ctx.scheduleOnce(writeTO, ref, msg)
            //ref ! msg
            case RemoteShard(_, ref) ⇒
              ctx.scheduleOnce(writeTO, ref.toUntyped, ConsistentHashableEnvelope(msg, chatId))
            //ref.toUntyped ! ConsistentHashableEnvelope(msg, chatId)
          }
          await(n, shards)
        }

        def await(chatId: Long, shards: Vector[Shard[ReplicatorProtocol]]): Behavior[WriteResponses] =
          Behaviors.receiveMessage[WriteResponses] {
            case AskForShards(replyTo) ⇒
              replyTo ! KnownShards(shards)
              ctx.log.warning("★ ★ ★  stop writer ★ ★ ★")
              Behaviors.stopped
            case WSuccess(_) ⇒
              write(chatId + 1L, shards)
            case WFailure(chatName, errorMsg) ⇒
              ctx.log.error(s"WriteFailure into $chatName because of $errorMsg")
              write(chatId + 1L, shards)
            case WTimeout(chatName) ⇒
              ctx.log.error(s"WriteTimeout into $chatName")
              write(chatId + 1L, shards)
            case StartWriting ⇒
              ctx.log.warning("Unexpected message: StartWriting in this state")
              Behaviors.unhandled
          }

        Behaviors.receiveMessage[WriteResponses] {
          case StartWriting ⇒
            write(0L, shards)
          case other ⇒
            ctx.log.warning("Unexpected message: " + other)
            Behaviors.unhandled
        }
      }
    }
}
