package chatter
package crdt

import scala.collection.Searching._

case class ChatTimeline(
    timeline: Vector[Message] = Vector.empty[Message],
    versions: VersionVector[Node] = VersionVector.empty[Node](Implicits.nodeOrd)
) extends akka.cluster.ddata.ReplicatedData {

  override type T = ChatTimeline

  def +(msg: Message, node: Node): ChatTimeline = {
    val ip: SearchResult = timeline.search(msg)(Implicits.msgOrd)
    if (ip.insertionPoint == 0)
      copy(msg +: timeline, versions + node)
    else if (ip.insertionPoint == timeline.size)
      copy(timeline :+ msg, versions + node)
    else {
      val (a, b) = timeline.splitAt(ip.insertionPoint)
      copy(a ++ (msg +: b), versions + node)
    }
  }

  //sort 2 sorted arrays in one array preserving messages with the same ts
  private def mergeConflicts(tlA: Vector[Message], tlB: Vector[Message]) = {
    val index = (0 until tlA.length).find(i ⇒ tlA(i) != tlB(i))
    if (index.isDefined) {
      val i = index.get
      val (same, a) = tlA.splitAt(i)
      val (_, b) = tlB.splitAt(i)
      var result = Vector.fill[Message](a.length + b.length)(null)
      var iA = a.length - 1
      var iB = b.length - 1
      var ind = result.length
      while (ind > 0) {
        ind -= 1
        val elem = if (iB < 0 || (iA >= 0 && a(iA).when >= b(iB).when)) {
          iA -= 1
          a(iA + 1)
        } else {
          iB -= 1
          b(iB + 1)
        }
        result = result.updated(ind, elem)
      }
      //println("up:" + result.map(_.when).mkString(",") + " :" + result.size)
      same ++ result
    } else if (tlA.size > tlB.size) tlA
    else tlB
  }

  /*
    Requires a bounded semilattice (or idempotent commutative monoid).
    We rely on commutivity to ensure that machine A merging with machine B yields the same result as machine B merging with machine A.
    We need associativity to ensure we obtain the correct result when three or more machines are merging data.
    We need an identity element to initialise empty counters.
    Finally, we need idempotency, to ensure that if two machines hold the same data
    in a per-machine ChatTimeline, merging them will not lead to an incorrect result.
  */
  override def merge(that: ChatTimeline): ChatTimeline = {
    //this.timeline union that.timeline
    if (versions < that.versions) {
      that
    } else if (versions > that.versions) {
      this
    } else if (versions <> that.versions) {
      //println(s"${versions.elems.mkString(",")} vs ${that.versions.elems.mkString(",")}")
      //val s = System.currentTimeMillis
      val r = mergeConflicts(timeline, that.timeline)
      //val l = System.currentTimeMillis - s
      //println(s"${versions.elems.map { case (n,v) => s"${n.port}:${v}"}.mkString(",")} vs ${that.versions.elems.map { case (n,v) => s"${n.port}:${v}"}.mkString(",")}")
      ChatTimeline(r, versions merge that.versions)
    } else this //means ==
  }
}