package chatter.serializer

import java.nio.charset.StandardCharsets._

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRefResolver
import akka.serialization.SerializerWithStringManifest
import chatter.actors.typed.{RChatTimelineReply, RGetFailureChatTimelineReply, RNotFoundChatTimelineReply, RWriteFailure, RWriteSuccess, RWriteTimeout, ReadChatTimeline, ReadReply, WriteMessage, WriteResponses}
import akka.actor.typed.scaladsl.adapter._
import akka.remote.WireFormats.ActorRefData
import akka.remote.serialization.ProtobufSerializer
import chatter.{Implicits, Message, Node}
import chatter.actors.typed.Replicator.v1._
import chatter.crdt.{ChatTimeline, VersionVector}
import com.google.protobuf

import scala.collection.immutable.TreeMap

class ChatTimelineReplicatorSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 99996

  override def manifest(obj: AnyRef): String = obj.getClass.getName

  /*
    https://discuss.lightbend.com/t/akka-typed-serialization/4336
    That is correct. ActorRefResolver provides toSerializationFormat for serialization to String and resolveActorRef for deserialization from String.
   */
  val aRefSerializer = ActorRefResolver(system.toTyped)

  //https://manuel.bernhardt.io/2018/07/20/akka-anti-patterns-java-serialization/
  //https://doc.akka.io/docs/akka/2.5.22/typed/cluster.html#serialization
  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case m: WriteMessage ⇒
        val replyToRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        //val pbRef = aRefSerializer.toSerializationFormat(m.replyTo).getBytes(UTF_8)
        val pb = WriteMessagePB(
          m.chatId,
          m.when,
          m.tz,
          m.userId,
          m.content,
          com.google.protobuf.ByteString.copyFrom(replyToRef.toByteArray)
        )
        //println(s"toBinary: ${m.replyTo}")
        pb.toByteArray
      case m: ReadChatTimeline ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        ReadChatTimelinePB(m.chatId, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)).toByteArray
      case m: RWriteSuccess ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteSuccessPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)).toByteArray
      case m: RWriteFailure ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteFailurePB(m.chatName, m.errorMsg, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)).toByteArray
      case m: RWriteTimeout ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteTimeoutPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)).toByteArray
      case m: RChatTimelineReply ⇒
        val versions = m.tl.versions.elems./:(TreeMap.empty[String, Long]) { (acc, c) ⇒
          acc + (s"${c._1.host}:${c._1.port}" → c._2)
        }
        val proto =
          m.tl.timeline.map(m ⇒ MessagePB(m.usrId, protobuf.ByteString.copyFrom(m.cnt.getBytes(UTF_8)), m.when, m.tz))
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RChatTimelineReplyPB(
          Some(ChatTimelinePB(proto, versions)),
          com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)
        ).toByteArray
      case m: RNotFoundChatTimelineReply ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RNotFoundChatTimelineReplyPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)).toByteArray
      case m: RGetFailureChatTimelineReply ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RGetFailureChatTimelineReplyPB(
          m.error,
          com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)
        ).toByteArray
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    if (manifest == classOf[WriteMessage].getName) {
      val pb  = WriteMessagePB.parseFrom(bytes)
      val ref = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      //val ref = aRefSerializer.resolveActorRef(pb.replyTo.toString(UTF_8))
      //println(s"fromBinary: ${ref}")
      WriteMessage(pb.chatId, pb.when, pb.tz, pb.authId, pb.content, ref)
    } else if (manifest == classOf[ReadChatTimeline].getName) {
      val pb    = ReadChatTimelinePB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      ReadChatTimeline(pb.chatId, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RWriteSuccess].getName) {
      val pb    = RWriteSuccessPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RWriteSuccess(pb.chatName, pbRef.toTyped[WriteResponses])
    } else if (manifest == classOf[RWriteFailure].getName) {
      val pb    = RWriteFailurePB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RWriteFailure(pb.chatName, pb.errorMsg, pbRef.toTyped[WriteResponses])
    } else if (manifest == classOf[RChatTimelineReply].getName) {
      val pb    = RChatTimelineReplyPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      val tlpb  = pb.history.get
      val tl = ChatTimeline(
        tlpb.history.toVector.map(m ⇒ Message(m.authId, m.cnt.toStringUtf8, m.when, m.tz)),
        VersionVector[Node](tlpb.versions./:(TreeMap.empty[Node, Long](Implicits.nodeOrd)) { (acc, m) ⇒
          val segments = m._1.split(":")
          acc + (Node(segments(0), segments(1).toInt) → m._2)
        })(Implicits.nodeOrd)
      )
      RChatTimelineReply(tl, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RNotFoundChatTimelineReply].getName) {
      val pb    = RNotFoundChatTimelineReplyPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RNotFoundChatTimelineReply(pb.chatName, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RGetFailureChatTimelineReply].getName) {
      val pb    = RGetFailureChatTimelineReplyPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RGetFailureChatTimelineReply(pb.error, pbRef.toTyped[ReadReply])
    } else
      throw new IllegalStateException(
        s"Deserialization for $manifest not supported. Check fromBinary method in ${this.getClass.getName} class."
      )
}
