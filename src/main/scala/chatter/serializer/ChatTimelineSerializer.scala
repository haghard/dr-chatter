package chatter
package serializer

import java.nio.charset.StandardCharsets

import akka.actor.ExtendedActorSystem
import akka.cluster.ddata.protobuf.SerializationSupport
import akka.serialization.Serializer
import chatter.crdt.{ ChatTimeline, VersionVector }
import chatter.serialization.pV0._

import scala.collection.immutable.TreeMap
import com.google.protobuf

class ChatTimelineSerializer(val system: ExtendedActorSystem) extends Serializer with SerializationSupport {

  override val identifier: Int = 99999

  override val includeManifest: Boolean = false

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case ct: ChatTimeline ⇒
        val versions = ct.versions.elems./:(TreeMap.empty[String, Long]) { (acc, c) ⇒
          acc + (s"${c._1.host}:${c._1.port}" -> c._2)
        }
        val proto = ct.timeline.map(m ⇒ MessagePB(
          m.authId,
          protobuf.ByteString.copyFrom(m.cnt.getBytes(StandardCharsets.UTF_8)), m.when, m.tz))
        ChatTimelinePB(proto, versions).toByteArray
    }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    //val start = System.currentTimeMillis
    val pb = ChatTimelinePB.parseFrom(bytes)
    val history = pb.history.toVector.map(m ⇒ Message(m.authId, m.cnt.toStringUtf8, m.when, m.tz))
    val versions = pb.versions./:(TreeMap.empty[Node, Long](Implicits.nodeOrd)) { (acc, m) ⇒
      val segments = m._1.split(":")
      acc + (Node(segments(0), segments(1).toInt) -> m._2)
    }
    ChatTimeline(history, VersionVector[Node](versions)(Implicits.nodeOrd))
  }
}