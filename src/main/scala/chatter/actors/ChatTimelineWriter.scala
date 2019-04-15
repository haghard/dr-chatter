package chatter
package actors

import java.time.ZoneId

import akka.cluster.Cluster
import akka.routing.ConsistentHashingGroup
import java.util.concurrent.ThreadLocalRandom

import akka.actor.typed.ActorRef
import akka.actor.{ Actor, ActorLogging, ActorSystem, Props }
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import scala.util.Random
import akka.actor.typed.scaladsl.adapter._
import chatter.actors.typed.ChatTimelineReader.{ KnownShards, ReadResponses }
import chatter.actors.typed.ChatTimelineReplicator.{ ReplicatorOps, WriteMessage }

object ChatTimelineWriter {

  sealed trait WriteResponses
  case class AskForShards(replyTo: ActorRef[ReadResponses]) extends WriteResponses
  case class WSuccess(chatName: String) extends WriteResponses
  case class WFailure(chatName: String, errorMsg: String) extends WriteResponses
  case class WTimeout(chatName: String) extends WriteResponses

  def routeeName(shard: String) = s"$shard-replicator"

  def props(system: ActorSystem, localShards: Vector[String], proxy: String, ids: Seq[Int]) =
    Props(new ChatTimelineWriter(system, localShards, proxy, ids))
}

class ChatTimelineWriter(system: ActorSystem, localShards: Vector[String], proxy: String, ids: Seq[Int]) extends Actor
  with ActorLogging with akka.actor.Timers {

  import ChatTimelineWriter._

  implicit val ex = context.dispatcher
  implicit val cluster = Cluster(context.system)

  val address = cluster.selfUniqueAddress.address
  val rnd = new Random(System.currentTimeMillis)

  val shards: Vector[Shard[ReplicatorOps]] = {
    val zero = new TreeSet[Shard[ReplicatorOps]]()((a: Shard[ReplicatorOps], b: Shard[ReplicatorOps]) ⇒ a.name.compareTo(b.name))
    localShards.foldLeft(zero)(_ + createLocalReplicator(_)) + createProxy(proxy)
  }.toVector

  val rocksDB = system.actorOf(RocksDBActor.props, RocksDBActor.name)

  override def preStart(): Unit = {
    timers.startSingleTimer(self.path.toString, 'StartWriting, 2.second)
  }

  def createLocalReplicator(shard: String) = {
    //supervision ???
    val name = routeeName(shard)
    log.info("★ ★ ★  local replicator for {}", shard)
    val ref: akka.actor.typed.ActorRef[ReplicatorOps] =
      system.spawn(chatter.actors.typed.ChatTimelineReplicator(system, shard, self.toTyped[WriteResponses]), name)
    LocalShard(shard, ref)
  }

  //we don't allocate any actors here just return an actorRef to an existing actor(replicator) allocated on another node
  def createProxy(shard: String) = {
    val shardName = routeeName(shard)
    log.info("★ ★ ★  remote replicator for {}", shard)
    val remoteProxyRouter = system.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances    = localShards.size,
          routeesPaths      = List(s"/user/$shardName"),
          allowLocalRoutees = false, //important
          useRoles          = shard)
      ).props(), s"router-$shard")

    import akka.actor.typed.scaladsl.adapter._
    RemoteShard(shard, remoteProxyRouter.toTyped[ReplicatorOps])
  }

  def awaitStart: Receive = {
    case AskForShards(replyTo) ⇒
      replyTo ! KnownShards(shards)
    case 'StartWriting ⇒
      write(1l)
  }

  def write(n: Long) = {
    val chatId = ids(ThreadLocalRandom.current.nextInt(ids.size))
    val userId = ThreadLocalRandom.current.nextLong(0l, 10l)
    val msg = WriteMessage(s"chat-$chatId", System.currentTimeMillis, ZoneId.systemDefault.getId, userId, rnd.nextString(1024 * 1))
    shards(chatId % shards.size) match {
      case LocalShard(_, ref) ⇒
        ref ! msg
      case RemoteShard(_, ref) ⇒
        import akka.actor.typed.scaladsl.adapter._
        ref.toUntyped ! ConsistentHashableEnvelope(message = msg, hashKey = chatId)
    }
    context.become(awaitResponse(n))
  }

  def awaitResponse(n: Long): Receive = {
    case AskForShards(replyTo) ⇒
      replyTo ! KnownShards(shards)
      log.warning("★ ★ ★  stop writer ★ ★ ★")
      context.stop(self)

    case WSuccess(_) ⇒
      write(n + 1l)

    /*if (context.system.scheduler ne null)
        context.system.scheduler.scheduleOnce(100.millis, new Runnable {
          override def run = write(n + 1l)
        })*/

    case WFailure(chatName, errorMsg) ⇒
      log.error(s"WriteFailure into ${chatName} because of ${errorMsg}")
      write(n + 1l)
    case WTimeout(chatName) ⇒
      log.error(s"WriteTimeout into ${chatName}")
      write(n + 1l)
  }

  override def receive: Receive = awaitStart
}