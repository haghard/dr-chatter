package chatter.serializer

import java.nio.charset.StandardCharsets

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import chatter.actors.typed.{ RChatTimelineReply, RGetFailureChatTimelineReply, RNotFoundChatTimelineReply, RWriteFailure, RWriteSuccess, RWriteTimeout, ReadChatTimeline, ReadReply, WriteMessage, WriteResponses }
import akka.actor.typed.scaladsl.adapter._
import akka.remote.WireFormats.ActorRefData
import akka.remote.serialization.ProtobufSerializer
import chatter.Message
import chatter.actors.typed.Replicator.v1._
import com.google.protobuf

class ChatTimelineReplicatorSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 99996

  override def manifest(obj: AnyRef): String = obj.getClass.getName

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case m: WriteMessage ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        val pb = WriteMessagePB(m.chatId, m.when, m.tz, m.authId, m.content,
                                com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
        //println(s"toBinary: ${m.replyTo}")
        pb.toByteArray
      case m: ReadChatTimeline ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        //println(s"toBinary: ${m.replyTo}")
        ReadChatTimelinePB(m.chatId, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RWriteSuccess ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteSuccessPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RWriteFailure ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteFailurePB(m.chatName, m.errorMsg, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RWriteTimeout ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RWriteTimeoutPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RChatTimelineReply ⇒
        val hist = m.history.map(m ⇒ MessagePB(
          m.authId,
          protobuf.ByteString.copyFrom(m.cnt.getBytes(StandardCharsets.UTF_8)), m.when, m.tz))
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RChatTimelineReplyPB(hist, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RNotFoundChatTimelineReply ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RNotFoundChatTimelineReplyPB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
      case m: RGetFailureChatTimelineReply ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        RGetFailureChatTimelineReplyPB(
          m.error,
          com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray)
        ).toByteArray
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    if (manifest == classOf[WriteMessage].getName) {
      val pb = WriteMessagePB.parseFrom(bytes)
      val ref = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      //println(s"fromBinary: ${ref}")
      WriteMessage(pb.chatId, pb.when, pb.tz, pb.authId, pb.content, ref)
    } else if (manifest == classOf[ReadChatTimeline].getName) {
      val pb = ReadChatTimelinePB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      //println(s"fromBinary: ${pbRef}")
      ReadChatTimeline(pb.chatId, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RWriteSuccess].getName) {
      val pb = RWriteSuccessPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RWriteSuccess(pb.chatName, pbRef.toTyped[WriteResponses])
    } else if (manifest == classOf[RWriteFailure].getName) {
      val pb = RWriteFailurePB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RWriteFailure(pb.chatName, pb.errorMsg, pbRef.toTyped[WriteResponses])
    } else if (manifest == classOf[RChatTimelineReply].getName) {
      val pb = RChatTimelineReplyPB.parseFrom(bytes)
      val hist = pb.history.toVector.map(p ⇒ Message(p.authId, p.cnt.toStringUtf8, p.when, p.tz))
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RChatTimelineReply(hist, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RNotFoundChatTimelineReply].getName) {
      val pb = RNotFoundChatTimelineReplyPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RNotFoundChatTimelineReply(pb.chatName, pbRef.toTyped[ReadReply])
    } else if (manifest == classOf[RGetFailureChatTimelineReply].getName) {
      val pb = RGetFailureChatTimelineReplyPB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      RGetFailureChatTimelineReply(pb.error, pbRef.toTyped[ReadReply])
    } else throw new IllegalStateException(
      s"Deserialization for $manifest not supported. Check fromBinary method in ${this.getClass.getName} class.")
  }
}
