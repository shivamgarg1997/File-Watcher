import java.io.PrintWriter
import java.nio.file.{FileSystem, FileSystems, Files, NoSuchFileException, Paths, StandardWatchEventKinds, WatchEvent}
import java.io._
import java.math.BigInteger
import java.security.{DigestInputStream, MessageDigest}

import org.apache.commons.io.FilenameUtils

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object watcher {

  val myDir = "/home/infoobjects/watchthis"
  val path = FileSystems.getDefault().getPath(myDir)
  val seperator : String = "/"
  val conf : SparkConf = new SparkConf().setAppName("FileWatcher").setMaster("local")
  val sc : SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  var oldHash : String =  ""

  def main(args: Array[String]): Unit = {


    val watchService = path.getFileSystem().newWatchService()

    path.register(watchService,StandardWatchEventKinds.ENTRY_CREATE,
                               StandardWatchEventKinds.ENTRY_DELETE,
                               StandardWatchEventKinds.ENTRY_MODIFY)
    println("running File Watcher => Watching  : " + myDir)
//    var listing = new File(myDir).list()
//
//    if(!listing.isEmpty){
//
//    }

    while(true){
      val watchKey = watchService.take()
      watchKey.pollEvents().asScala.foreach( e => {
        printEvent(e)

      })

      if (!watchKey.reset()) {
        watchKey.cancel()
        watchService.close()
        break
      }
    }
  }

  def printEvent(event: WatchEvent[_]) : Unit = {
    val kind = event.kind()

    val fileName : String = event.context().toString
   // println("FileName : " + fileName)

    if(!fileName.endsWith(".tlr") && !fileName.charAt(0).equals(".")){

      if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE)){

          println(kind.toString)
          val hash : String =  findHash(fileName)
          val mostFreqWord : String = mostFrequentWord(fileName)
          val lines : Long = numberOfLines(fileName)
          // val noOfWords : String = numberOfWords(fileName)
          val data : String  = "FILE : " + fileName +
            "\nEVENT : " + kind +
            "\nHash : " + hash +
            "\nMost_Frequent Word : " + mostFreqWord +
            "\nNumber Of Lines: " + lines +
            "\n"
          println("creating trailer file for : " + fileName)
          Files.write(Paths.get(myDir + "/" + getFileName(fileName) + ".tlr"),data.getBytes())
          oldHash = hash

      } else if(kind.equals(StandardWatchEventKinds.ENTRY_DELETE)){

        println(kind.toString)
        deleteTrailerFile(fileName)
        println("deleting file : " + fileName)

      } else if(kind.equals(StandardWatchEventKinds.ENTRY_MODIFY)){

        val hash : String =  findHash(fileName)
        if(!oldHash.equals(hash)) {
          val mostFreqWord: String = mostFrequentWord(fileName)
          val data: String = "FILE : " + fileName +
            "\nEVENT : " + kind +
            "\nHash : " + hash +
            "\nMost_Frequent Word : " + mostFreqWord
          println("modifying trailer file for : " + fileName)
          Files.write(Paths.get(myDir + "/" + getFileName(fileName) + ".tlr"), data.getBytes())
        }
      }
    }
  }

  def findHash(fileName: String) : String = {
    val buffer = new Array[Byte](8192)
    val sha = MessageDigest.getInstance("SHA-1")
    val dis = new DigestInputStream( new FileInputStream(new File(myDir +seperator + fileName)), sha)

    try {
      while(dis.read(buffer) != -1) {}
    } finally {
      dis.close()
    }
    val hash :String = sha.digest.map("%02x".format(_)).mkString
   // println("Hashed contents : " + hash)
    hash
  }

  def mostFrequentWord(fileName: String) : String = {
    val contents = sc.textFile(myDir + seperator + fileName)
    if(contents.isEmpty()){
      return "File is Empty"
    }
    val wc = contents.flatMap(line => line.split("!| |,|;"))
                      .map(word => (word,1))
                        .reduceByKey(_+_)
    val wc_swap = wc.map(_.swap)
    val highFreq = wc_swap.sortByKey(false,1)
    highFreq.first().toString
  }

  def numberOfLines(fileName : String) : Long = {
    try{
      sc.textFile(myDir + seperator + fileName).count()
    } catch {
      case e : Exception => return 0
    }
  }

  def deleteTrailerFile(fileName: String) : Unit = {
    try{
      Files.deleteIfExists(Paths.get(myDir + seperator + getFileName(fileName) + ".tlr"))
    } catch {
      case e : NoSuchFileException => e.printStackTrace()
    }
  }

  def getFileName(fileName: String) : String = {
    var newFileName : String = ""
    if (fileName.indexOf(".") > 0) {
      newFileName = fileName.substring(0, fileName.lastIndexOf("."))
    }
    newFileName
  }
}
