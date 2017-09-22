package com.hakan

import java.util.regex.{Matcher, Pattern}

class NetstatParser extends Serializable {

  private val oneWord = "(\\S+)"
  val regex = s"$oneWord,$oneWord,$oneWord,$oneWord,$oneWord,$oneWord"
  private val p = Pattern.compile(regex)


  def parseRecord(record: String): Option[NetstatRecord] = {
    val matcher = p.matcher(record)  //basic line check
    if (matcher.find) {
      Some(buildNetstatRecord(matcher))
    } else {
      None
    }
  }

  //TODO: girilen date'ten 15 gün eskiye dön, ondan yeni mi diye kontrol et
  def filterDate(x: Option[NetstatRecord], date: String): Boolean = {
    if (x.isDefined)
      true //{if(x.get.date.equals(date)) true else false }
    else
      false
  }

  //only necessary fields
  private def buildNetstatRecord(matcher: Matcher): NetstatRecord = {

    //atribute remediation
    NetstatRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6)
    )
  }


  def parseLine_NetstatTuple(x: Option[NetstatRecord]) = {

    //katrnora04,10.82.31.9,1521,10.80.33.208,52259,oracle

    var ip_s = x.get.ip_1
    var ip_d = x.get.ip_2
    val port_1 = x.get.port_1
    val port_2 = x.get.port_2

    var port = port_2
    if (port_1 < port_2) {
      port = port_1
      ip_s=x.get.ip_2
      ip_d=x.get.ip_1
    }

    (x.get.hostname, ip_s, ip_d, port, x.get.process)

  }



}
