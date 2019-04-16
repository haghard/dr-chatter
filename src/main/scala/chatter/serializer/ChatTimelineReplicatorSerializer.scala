package chatter.serializer

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import chatter.actors.typed.{ ReadChatTimeline, WriteMessage }
import akka.actor.typed.scaladsl.adapter._
import akka.remote.WireFormats.ActorRefData
import akka.remote.serialization.ProtobufSerializer
import chatter.actors.typed.ReplicatorProtocol.v1._

class ChatTimelineReplicatorSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 99996

  override def manifest(obj: AnyRef): String = obj.getClass.getName

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case m: WriteMessage ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        val pb = WriteMessagePB(m.chatName, m.when, m.tz, m.authId, m.content,
                                com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
        //println(s"toBinary: ${m.replyTo.path}")
        pb.toByteArray
      case m: ReadChatTimeline ⇒
        val pbRef = ProtobufSerializer.serializeActorRef(m.replyTo.toUntyped)
        //println(s"toBinary: ${m.replyTo.path}")
        ReadChatTimelinePB(m.chatName, com.google.protobuf.ByteString.copyFrom(pbRef.toByteArray))
          .toByteArray
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    if (manifest == classOf[WriteMessage].getName) {
      val pb = WriteMessagePB.parseFrom(bytes)
      val ref = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      //println(s"fromBinary: ${ref.path}")
      WriteMessage(pb.chatName, pb.when, pb.tz, pb.authId, pb.content, ref)
    } else if (manifest == classOf[ReadChatTimeline].getName) {
      val pb = ReadChatTimelinePB.parseFrom(bytes)
      val pbRef = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(pb.replyTo.toByteArray))
      //println(s"fromBinary: ${pbRef.path}")
      ReadChatTimeline(pb.chatName, pbRef)
    } else throw new IllegalStateException(
      s"Deserialization for $manifest not supported. Check fromBinary method in ${this.getClass.getName} class.")
  }
}
