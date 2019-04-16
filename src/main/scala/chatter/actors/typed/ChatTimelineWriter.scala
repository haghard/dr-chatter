package chatter
package actors
package typed

import java.time.ZoneId
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.actor.typed.{ ActorRef, Behavior }
import akka.actor.typed.scaladsl.Behaviors
import chatter.actors.typed.ChatTimelineReplicator.{ ReplCommand, WriteMessage }

import scala.util.Random
import scala.collection.immutable.TreeSet
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import akka.routing.ConsistentHashingGroup

import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import chatter.actors.typed.ChatTimelineReader.{ KnownShards, ReadResponses }

object ChatTimelineWriter {

  sealed trait WriteResponses
  case object StartWriting extends WriteResponses
  case class AskForShards(replyTo: ActorRef[ReadResponses]) extends WriteResponses
  case class WSuccess(chatName: String) extends WriteResponses
  case class WFailure(chatName: String, errorMsg: String) extends WriteResponses
  case class WTimeout(chatName: String) extends WriteResponses

  def apply(system: ActorSystem, localShards: Vector[String], proxy: String, ids: Seq[Int]): Behavior[WriteResponses] =
    Behaviors.setup { ctx ⇒

      val writeTO = 30.millis
      val rnd = new Random(System.currentTimeMillis)

      system.actorOf(RocksDBActor.props, RocksDBActor.name)
      //system.actorOf()

      Behaviors.withTimers[WriteResponses] { timers ⇒
        timers.startSingleTimer("", StartWriting, 2.second)

        def routeeName(shard: String) = s"$shard-replicator"

        def createLocalReplicator(shard: String) = {
          val name = routeeName(shard)
          ctx.log.info("★ ★ ★  local replicator for {}", shard)
          val ref: akka.actor.typed.ActorRef[ReplCommand] =
            system.spawn(chatter.actors.typed.ChatTimelineReplicator(system, shard, ctx.self), name)
          LocalShard(shard, ref)
        }

        def createProxy(shard: String) = {
          val shardName = routeeName(shard)
          ctx.log.info("★ ★ ★  remote replicator for {}", shard)
          val remoteProxyRouter = system.actorOf(
            ClusterRouterGroup(
              ConsistentHashingGroup(Nil),
              ClusterRouterGroupSettings(
                totalInstances    = localShards.size,
                routeesPaths      = List(s"/user/$shardName"),
                allowLocalRoutees = false, //important
                useRoles          = shard)
            ).props(), s"router-$shard")
          RemoteShard(shard, remoteProxyRouter.toTyped[ReplCommand])
        }

        def write(n: Long, shards: Vector[Shard[ReplCommand]]): Behavior[WriteResponses] = {
          val chatId = ids(ThreadLocalRandom.current.nextInt(ids.size))
          val userId = ThreadLocalRandom.current.nextLong(0l, 10l)
          val msg = WriteMessage(s"chat-$chatId", System.currentTimeMillis, ZoneId.systemDefault.getId, userId, rnd.nextString(1024 * 1))
          shards(chatId % shards.size) match {
            case LocalShard(_, ref) ⇒
              ctx.scheduleOnce(writeTO, ref, msg)
            //ref ! msg
            case RemoteShard(_, ref) ⇒
              ctx.scheduleOnce(writeTO, ref.toUntyped, ConsistentHashableEnvelope(msg, chatId))
            //ref.toUntyped ! ConsistentHashableEnvelope(msg, chatId)
          }
          await(n, shards)
        }

        def await(chatId: Long, shards: Vector[Shard[ReplCommand]]): Behavior[WriteResponses] =
          Behaviors.receiveMessage[WriteResponses] {
            case AskForShards(replyTo) ⇒
              replyTo ! KnownShards(shards)
              ctx.log.warning("★ ★ ★  stop writer ★ ★ ★")
              Behaviors.stopped
            case WSuccess(_) ⇒
              write(chatId + 1l, shards)
            case WFailure(chatName, errorMsg) ⇒
              ctx.log.error(s"WriteFailure into ${chatName} because of ${errorMsg}")
              write(chatId + 1l, shards)
            case WTimeout(chatName) ⇒
              ctx.log.error(s"WriteTimeout into ${chatName}")
              write(chatId + 1l, shards)
            case StartWriting ⇒
              ctx.log.warning("Unexpected message: StartWriting in this state")
              Behaviors.unhandled
          }

        Behaviors.receiveMessage[WriteResponses] {
          case StartWriting ⇒
            val shards: Vector[Shard[ReplCommand]] = {
              val zero = new TreeSet[Shard[ReplCommand]]()((a: Shard[ReplCommand], b: Shard[ReplCommand]) ⇒ a.name.compareTo(b.name))
              localShards./:(zero)(_ + createLocalReplicator(_)) + createProxy(proxy)
            }.toVector
            write(0l, shards)
          case other ⇒
            ctx.log.warning("Unexpected message: " + other)
            Behaviors.unhandled
        }
      }
    }
}
