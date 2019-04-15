package chatter

import akka.actor.ActorSystem
import akka.cluster.Cluster
import chatter.actors.{ ChatTimelineReader, ChatTimelineWriter }
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

//runMain chatter.Runner
//sbt "runMain chatter.Runner"
object Runner extends App {

  val systemName = "dr-chatter"

  //each node holds 2/3 of all data
  val shards = Vector("alpha", "betta", "gamma")
  val ids = Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

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

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(rolesConfig(0, 1)).withFallback(commonConfig).withFallback(ConfigFactory.load()))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(rolesConfig(0, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))
  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(rolesConfig(1, 2)).withFallback(commonConfig).withFallback(ConfigFactory.load()))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)

  val w1 = node1.actorOf(ChatTimelineWriter.props(node1, Vector(shards(0), shards(1)), shards(2), ids), "alpha-writer")
  val w2 = node2.actorOf(ChatTimelineWriter.props(node2, Vector(shards(0), shards(2)), shards(1), ids), "betta-writer")
  val w3 = node3.actorOf(ChatTimelineWriter.props(node3, Vector(shards(1), shards(2)), shards(0), ids), "gamma-writer")

  Helpers.wait(10.second * 1)

  //node1.actorOf(ChatTimelineReader.props(w1), "alpha-reader")
  //node2.actorOf(ChatTimelineReader.props(w2), "betta-reader")
  //node3.actorOf(ChatTimelineReader.props(w3), "gamma-reader")

  Helpers.wait(15.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
}