package chatter

import akka.cluster.Cluster
import chatter.actors.RocksDBActor
import com.typesafe.config.ConfigFactory
import akka.routing.ConsistentHashingGroup
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import chatter.actors.typed.{ ChatTimelineReader, ChatTimelineReplicator, ChatTimelineWriter, ReplicatorCommand }

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._

//runMain chatter.Runner
//sbt "runMain chatter.Runner"
object Runner extends App {

  val systemName = "timeline"

  val shards = Vector("alpha", "betta", "gamma")
  val ids = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

  val writeDuration = 30.second * 1

  val commonConfig = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            auto-down-unreachable-after = 1 s
          }
         actor.provider = cluster
         remote.artery {
           enabled = on
           transport = tcp
           canonical.hostname = 127.0.0.1
         }
         cluster.jmx.multi-mbeans-in-same-jvm=on
         actor.warn-about-java-serializer-usage=off
       }
    """)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  def rolesConfig(ind0: Int, ind1: Int) =
    ConfigFactory.parseString(s"akka.cluster.roles = [${shards(ind0)}, ${shards(ind1)}]")

  def spawnReplicator(shard: String, ctx: ActorContext[Unit]) = {
    val ref: akka.actor.typed.ActorRef[ReplicatorCommand] =
      ctx.spawn(ChatTimelineReplicator(shard), shard)
    ctx.system.log.info("★ ★ ★  local replicator for {}", ref.path)
    LocalShard(shard, ref)
  }

  def spawnProxyReplicator(shard: String, ctx: ActorContext[Unit]) = {
    val remoteProxyRouter = ctx.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances    = 2,
          routeesPaths      = List(s"/user/$shard"),
          allowLocalRoutees = false, //important
          useRoles          = shard)
      ).props(), s"proxy-$shard")
    ctx.system.log.info("★ ★ ★  remote replicator for {}", remoteProxyRouter.path)
    RemoteShard(shard, remoteProxyRouter.toTyped[ReplicatorCommand])
  }

  def spawnShards(local: Seq[String], remote: String, ctx: ActorContext[Unit]): Vector[Shard[ReplicatorCommand]] = {
    val ss: Vector[Shard[ReplicatorCommand]] = {
      val zero = new TreeSet[Shard[ReplicatorCommand]]()((a: Shard[ReplicatorCommand], b: Shard[ReplicatorCommand]) ⇒ a.name.compareTo(b.name))
      local./:(zero)(_ + spawnReplicator(_, ctx)) + spawnProxyReplicator(remote, ctx)
    }.toVector
    ss
  }

  /*
      The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
      For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.

      RF for this setup is 2
      Each node holds 2/3 of all data
  */
  val node1 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("init-2550", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒
          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          val ss = spawnShards(Seq(shards(0), shards(1)), shards(2), ctx)
          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")

          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")

          Behaviors.ignore
      }
    }

  }, systemName, portConfig(2550).withFallback(rolesConfig(0, 1)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node2 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("init-2551", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒
          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          val ss = spawnShards(Seq(shards(0), shards(2)), shards(1), ctx)
          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")

          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")

          Behaviors.ignore
      }
    }
  }, systemName, portConfig(2551).withFallback(rolesConfig(0, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node3 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("init-2552", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒
          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          val ss = spawnShards(Seq(shards(1), shards(2)), shards(0), ctx)
          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")

          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")

          Behaviors.ignore
      }
    }
  }, systemName, portConfig(2552).withFallback(rolesConfig(1, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node1Cluster = Cluster(node1.toUntyped)
  val node2Cluster = Cluster(node2.toUntyped)
  val node3Cluster = Cluster(node3.toUntyped)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1.toUntyped, node2.toUntyped, node3.toUntyped)

  Helpers.wait(writeDuration)

  Helpers.wait(20.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
}