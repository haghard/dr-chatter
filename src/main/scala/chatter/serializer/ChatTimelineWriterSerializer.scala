package chatter
package serializer

import akka.actor.ExtendedActorSystem
import akka.remote.serialization.ProtobufSerializer
import akka.serialization.SerializerWithStringManifest
import chatter.actors.typed.WriterProtocol.v1._
import akka.actor.typed.scaladsl.adapter._
import akka.remote.WireFormats.ActorRefData
import chatter.actors.typed.{ AskForShards, ReadReply, WFailure, WSuccess, WTimeout }

class ChatTimelineWriterSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {

  override val identifier: Int = 99998

  override def manifest(obj: AnyRef): String = obj.getClass.getName

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case AskForShards(replyTo) ⇒
        val pb = ProtobufSerializer.serializeActorRef(replyTo.toUntyped)
        //println(s"toBinary: ${replyTo.path} ")
        pb.toByteArray
      case WSuccess(name) ⇒
        //println(s"toBinary: WSuccess ${name} ")
        WSuccessPB(name).toByteArray
      case WFailure(name, error) ⇒
        WFailurePB(name, error).toByteArray
      case WTimeout(name) ⇒
        WTimeoutPB(name).toByteArray
      case _ ⇒
        throw new IllegalStateException(s"Serialization for $obj not supported. Check toBinary in ${this.getClass.getName}.")
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    if (manifest == classOf[AskForShards].getName) {
      val ref = ProtobufSerializer.deserializeActorRef(system, ActorRefData.parseFrom(bytes)).toTyped[ReadReply]
      //println(s"fromBinary: ${ref.path}")
      AskForShards(ref)
    } else if (manifest == classOf[WSuccess].getName) {
      WSuccess(WSuccessPB.parseFrom(bytes).chatName)
    } else if (manifest == classOf[WFailure].getName) {
      val pb = WFailurePB.parseFrom(bytes)
      WFailure(pb.chatName, pb.errorMsg)
    } else if (manifest == classOf[WTimeout].getName) {
      WTimeout(WTimeoutPB.parseFrom(bytes).chatName)
    } else throw new IllegalStateException(
      s"Deserialization for $manifest not supported. Check fromBinary method in ${this.getClass.getName} class.")
  }
}
