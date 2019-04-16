package chatter

import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.Cluster
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import akka.routing.ConsistentHashingGroup
import chatter.actors.RocksDBActor
import chatter.actors.typed.ChatTimelineReplicator.ReplCommand
import chatter.actors.typed.{ ChatTimelineReader, ChatTimelineWriter }
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._

//runMain chatter.Runner
//sbt "runMain chatter.Runner"
object Runner extends App {

  val systemName = "dr-chatter"

  //each node holds 2/3 of all data
  val shards = Vector("alpha", "betta", "gamma")
  val ids = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

  val writeDuration = 10.second * 1

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

  /*
    The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
    For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.
  */

  import akka.actor.typed.scaladsl.adapter._

  val node1 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("a", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒
          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          def localReplicator(shard: String) = {
            val ref: akka.actor.typed.ActorRef[ReplCommand] =
              ctx.spawn(chatter.actors.typed.ChatTimelineReplicator(shard), shard)
            ctx.system.log.info("★ ★ ★  local replicator for {}", ref.path)
            LocalShard(shard, ref)
          }

          def proxyReplicator(shard: String) = {
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
            RemoteShard(shard, remoteProxyRouter.toTyped[ReplCommand])
          }

          val ss: Vector[Shard[ReplCommand]] = {
            val zero = new TreeSet[Shard[ReplCommand]]()((a: Shard[ReplCommand], b: Shard[ReplCommand]) ⇒ a.name.compareTo(b.name))
            Seq(shards(0), shards(1))./:(zero)(_ + localReplicator(_)) + proxyReplicator(shards(2))
          }.toVector

          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")
          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")
          Behaviors.ignore
      }
    }

  }, systemName, portConfig(2550).withFallback(rolesConfig(0, 1)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node2 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("a", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒

          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          def localReplicator(shard: String) = {
            val ref: akka.actor.typed.ActorRef[ReplCommand] =
              ctx.spawn(chatter.actors.typed.ChatTimelineReplicator(shard), shard)
            ctx.system.log.info("★ ★ ★  local replicator for {}", ref.path.toString)
            LocalShard(shard, ref)
          }

          def proxyReplicator(shard: String) = {
            val remoteProxyRouter = ctx.actorOf(
              ClusterRouterGroup(
                ConsistentHashingGroup(Nil),
                ClusterRouterGroupSettings(
                  totalInstances    = 2,
                  routeesPaths      = List(s"/user/$shard"),
                  allowLocalRoutees = false, //important
                  useRoles          = shard)
              ).props(), s"proxy-$shard")
            ctx.system.log.info("★ ★ ★  remote replicator for {}", remoteProxyRouter.path.toString)
            RemoteShard(shard, remoteProxyRouter.toTyped[ReplCommand])
          }

          // /user/betta/replicator
          val ss: Vector[Shard[ReplCommand]] = {
            val zero = new TreeSet[Shard[ReplCommand]]()((a: Shard[ReplCommand], b: Shard[ReplCommand]) ⇒ a.name.compareTo(b.name))
            Seq(shards(0), shards(2))./:(zero)(_ + localReplicator(_)) + proxyReplicator(shards(1))
          }.toVector

          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")
          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")

          Behaviors.ignore
      }
    }
  }, systemName, portConfig(2551).withFallback(rolesConfig(0, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node3 = akka.actor.typed.ActorSystem(Behaviors.setup[Unit] { ctx ⇒
    Behaviors.withTimers[Unit] { timers ⇒
      timers.startSingleTimer("a", (), 2.seconds)
      Behaviors.receiveMessage {
        case _: Unit ⇒

          ctx.actorOf(RocksDBActor.props, RocksDBActor.name)

          def localReplicator(shard: String) = {
            val ref: akka.actor.typed.ActorRef[ReplCommand] =
              ctx.spawn(chatter.actors.typed.ChatTimelineReplicator(shard), shard)
            ctx.log.info("★ ★ ★  local replicator for {}", ref.path)
            LocalShard(shard, ref)
          }

          def proxyReplicator(shard: String) = {
            val remoteProxyRouter = ctx.actorOf(
              ClusterRouterGroup(
                ConsistentHashingGroup(Nil),
                ClusterRouterGroupSettings(
                  totalInstances    = 2,
                  routeesPaths      = List(s"/user/$shard"),
                  allowLocalRoutees = false, //important
                  useRoles          = shard)
              ).props(), s"proxy-$shard")
            ctx.log.info("★ ★ ★  remote replicator for {}", remoteProxyRouter.path)
            RemoteShard(shard, remoteProxyRouter.toTyped[ReplCommand])
          }

          val ss: Vector[Shard[ReplCommand]] = {
            val zero = new TreeSet[Shard[ReplCommand]]()((a: Shard[ReplCommand], b: Shard[ReplCommand]) ⇒ a.name.compareTo(b.name))
            Seq(shards(1), shards(2))./:(zero)(_ + localReplicator(_)) + proxyReplicator(shards(0))
          }.toVector

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

  Helpers.wait(10.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
}