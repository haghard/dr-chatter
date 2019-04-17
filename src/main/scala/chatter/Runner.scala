package chatter

import akka.cluster.Cluster
import chatter.actors.RocksDBBehavior
import com.typesafe.config.ConfigFactory
import akka.routing.ConsistentHashingGroup
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.cluster.routing.{ ClusterRouterGroup, ClusterRouterGroupSettings }
import chatter.actors.typed.{ ChatTimelineReader, ChatTimelineReplicator, ChatTimelineWriter, ReplicatorCommand }

import scala.collection.immutable.TreeSet
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._
import chatter.actors.RocksDBBehavior.InitRocksDb

/*
You can use Cluster Sharding and DData with roles. So, let's say that you go with 10 roles, 10,000 entities in each role. You would then start Replicators on the nodes with corresponding nodes. You would also start Sharding on the nodes with corresponding roles. On a node that doesn't have the a role you would start a sharding proxy for such role.

When you want to send a message to an entity you first need to decide which role to use for that message. Can be simple hashCode modulo algorithm. Then you delegate the message to the corresponding Sharding region or proxy actor.

You have defined the Props for the entities and there you pass in the Replicator corresponding to the role that the entity belongs to, i.e. the entity takes the right Replicator ActorRef as constructor parameter.

If you don't need the strict guarantees of "only one entity" that Cluster Sharding provides, and prefer better availability in case of network partitions, you could use a consistent hashing group router instead of Cluster Sharding. You would have one router per role, and decide router similar as above. Then the entities (routees of the router) would have to subscribe to changes from DData to get notified of when a peer entity has changed something, since you can have more than one alive at the same time.

 */

//runMain chatter.Runner
//sbt "runMain chatter.Runner"
object Runner extends App {

  val systemName = "timeline"

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

  def spawnReplicator(shard: String, ctx: ActorContext[Unit]) = {
    val ref: akka.actor.typed.ActorRef[ReplicatorCommand] =
      ctx.spawn(ChatTimelineReplicator(shard), shard)
    ctx.system.log.info("★ ★ ★  spawn local replicator for {}", ref.path)
    LocalShard(shard, ref)
  }

  def spawnProxyReplicator(remoteShardName: String, localShardsSize: Int, ctx: ActorContext[Unit]) = {
    val remoteProxyRouter = ctx.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances    = localShardsSize,
          routeesPaths      = List(s"/user/$remoteShardName"),
          allowLocalRoutees = false, //important
          useRoles          = remoteShardName)
      ).props(), s"proxy-$remoteShardName")
    ctx.system.log.info("★ ★ ★  spawn remote replicator for {}", remoteProxyRouter.path)
    RemoteShard(remoteShardName, remoteProxyRouter.toTyped[ReplicatorCommand])
  }

  def spawnShards(local: Seq[String], remote: String, ctx: ActorContext[Unit]): Vector[Shard[ReplicatorCommand]] = {
    val r: Vector[Shard[ReplicatorCommand]] = {
      val zero = new TreeSet[Shard[ReplicatorCommand]]()((a: Shard[ReplicatorCommand], b: Shard[ReplicatorCommand]) ⇒ a.name.compareTo(b.name))
      local./:(zero)(_ + spawnReplicator(_, ctx)) + spawnProxyReplicator(remote, local.size, ctx)
    }.toVector
    r
  }

  /*
      The number of failures that can be tolerated is equal to (Replication factor - 1) /2.
      For example, with 3x replication, one failure can be tolerated; with 5x replication, two failures, and so on.

      RF for this setup is 2
      Each node holds 2/3 of all data
  */
  val node1 = akka.actor.typed.ActorSystem(
    Behaviors.setup[Unit] { context ⇒
      context.log.info("{} started and ready to join cluster", context.system.name)

      Behaviors.withTimers[Unit] { timers ⇒
        timers.startSingleTimer("init-2550", (), 2.seconds)

        Behaviors.receive { (context, _) ⇒
          context.spawn(new RocksDBBehavior(context), RocksDBBehavior.name)
          //case _: Unit ⇒
          //ctx.actorOf(RocksDBBehavior.props, RocksDBBehavior.name)

          val ss = spawnShards(Seq(shards(0), shards(1)), shards(2), context)
          val w = context.spawn(ChatTimelineWriter(ss, ids), "writer")

          context.spawn(ChatTimelineReader(w, writeDuration), "reader")

          //Behaviors.setup[AskRocksDb] { c ⇒

          Behaviors.ignore
        }
      }
    }, systemName, portConfig(2550).withFallback(rolesConfig(0, 1)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node2 = akka.actor.typed.ActorSystem(
    Behaviors.setup[Unit] { ctx ⇒
      Behaviors.withTimers[Unit] { timers ⇒
        timers.startSingleTimer("init-2551", (), 2.seconds)

        Behaviors.receive { (context, _) ⇒
          context.spawn(new RocksDBBehavior(context), RocksDBBehavior.name)

          val ss = spawnShards(Seq(shards(0), shards(2)), shards(1), ctx)
          val w = ctx.spawn(ChatTimelineWriter(ss, ids), "writer")

          ctx.spawn(ChatTimelineReader(w, writeDuration), "reader")

          Behaviors.ignore
        }
      }
    }, systemName, portConfig(2551).withFallback(rolesConfig(0, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node3 = akka.actor.typed.ActorSystem(
    Behaviors.setup[Unit] { ctx ⇒
      Behaviors.withTimers[Unit] { timers ⇒
        timers.startSingleTimer("init-2552", (), 2.seconds)

        Behaviors.receive { (context, _) ⇒
          context.spawn(new RocksDBBehavior(context), RocksDBBehavior.name)

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