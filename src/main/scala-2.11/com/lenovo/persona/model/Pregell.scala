//package com.lenovo.persona.model
//
//import org.apache.spark.Logging
//import org.apache.spark.graphx.Pregel.logInfo
//import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}
//
//import scala.reflect.ClassTag
//
//object Pregell extends Logging{
//
//  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
//  (graph: Graph[VD, ED],
//   initialMsg: A,
//   maxIterations: Int = Int.MaxValue,
//   activeDirection: EdgeDirection = EdgeDirection.Either)
//  (vprog: (VertexId, VD, A) => VD,
//   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
//   mergeMsg: (A, A) => A)
//  : Graph[VD, ED] =
//  {
//    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
//    // compute the messages
//    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
//    var activeMessages = messages.count()
//    // Loop
//    var prevG: Graph[VD, ED] = null
//    var i = 0
//    while (activeMessages > 0 && i < maxIterations) {
//      // Receive the messages and update the vertices.
//      prevG = g
//      g = g.joinVertices(messages)(vprog).cache()
//
//      val oldMessages = messages
//      // Send new messages, skipping edges where neither side received a message. We must cache
//      // messages so it can be materialized on the next line, allowing us to uncache the previous
//      // iteration.
//      messages = g.mapReduceTriplets(
//        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
//      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
//      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
//      // and the vertices of g).
//      activeMessages = messages.count()
//
//      logInfo("Pregel finished iteration " + i)
//
//      // Unpersist the RDDs hidden by newly-materialized RDDs
//      oldMessages.unpersist(blocking = false)
//      prevG.unpersistVertices(blocking = false)
//      prevG.edges.unpersist(blocking = false)
//      // count the iteration
//      i += 1
//    }
//
//    g
//  }
//
//}
