/**
  * Created by zhangy4 on 1/12/17.
  */

package org.screp.fastqutil

import scala.io.Source

case class FastqRecord(id: String, seq: String, optid: String, qual: String)

class FastqFile (val filename: String) {
  private val fastqSource = Source.fromFile(filename)
  val fastqRecordIter = fastqSource.getLines().grouped(4).map((rec: Seq[String]) =>
    rec match {
      case Seq(id: String, seq: String, optid: String, qual: String) => FastqRecord(id, seq, optid, qual)
    }
  )
}

object FastqFile {
  def apply(filename: String) = {
    new FastqFile(filename)
  }
}