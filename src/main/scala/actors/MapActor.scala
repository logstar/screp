package org.screp.actors

import akka.actor.{Actor, ActorRef, Props}

import scala.util.matching.Regex
import org.screp.{EndOfExec, EndOfRecord, MapData}
import org.screp.fastqutil.FastqRecord

/**
  * Created by zhangy4 on 1/12/17.
  */

class MapActor(regex: Regex, reducer: ActorRef) extends Actor {
  def receive: Receive = {
    case fastqRec: FastqRecord => reducer ! matchFastqRec(fastqRec)
    case _: EndOfRecord => {
      reducer ! EndOfExec()
      // println("Mapper: eor")
      context stop self
    }
  }

  def matchFastqRec(fastqRec: FastqRecord) : MapData = {
    this.regex.findFirstIn(fastqRec.seq) match {
      case Some(matchedString) => MapData(Some(fastqRec))
      case None => MapData(None)
    }
  }

}

object MapActor {
  def props(regex: Regex, reducer: ActorRef): Props =
    Props(new MapActor(regex, reducer))
}