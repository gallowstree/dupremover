import java.io.{File, FileInputStream, FileWriter}
import java.security.{DigestInputStream, MessageDigest}

import scala.xml._
import scala.xml.transform.{RewriteRule, RuleTransformer}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val directory = new File(args(0))

    if (!directory.isDirectory) {
      println(s"Not a directory [$directory]")
      return ;
    }

    val uniqueFiles = getUniqueFiles(directory)

    writeDuplicateReport(uniqueFiles)

    deleteDuplicates(uniqueFiles)

    updateSyncFilter(uniqueFiles.map(_.copies.map(_.getName)).flatMap(_.toList))

  }

  def getUniqueFiles(directory:File): Iterable[UniqueFile] = {

    val sha1 = MessageDigest.getInstance("SHA1")

    val filesByHash = directory
      .listFiles()
      .filter(_.isFile)
      .groupBy(file => computeHash(file, sha1))

    filesByHash.map((kv: (String, Array[File])) => {
      val sorted = kv._2.sortBy(_.getName)
      val filesToDelete = sorted.drop(1)
      new UniqueFile(sorted(0), kv._1, filesToDelete)
    })
  }

  def writeDuplicateReport(uniqueFiles: Iterable[UniqueFile]): Unit = {
    val fileWriter = new FileWriter("/home/alejandro/Documents/DATA/elec2019/ftp/dups/duplicates.txt", true)

    try {
      uniqueFiles.foreach(entry => fileWriter.write(entry.toString + "\n"))
    } finally fileWriter.close()

  }

  def deleteDuplicates(uniqueFiles: Iterable[UniqueFile]) {
    uniqueFiles.foreach(entry => entry.copies.foreach(_.delete()))
  }

  def updateSyncFilter(filesToIgnore: Iterable[String]) {
    val path = "/home/alejandro/elec2019/tse-prod-dedup.ffs_batch"
    val xmlFile = XML.loadFile(path)

    val addExcludeItems = new AddExcludeItems(filesToIgnore.map(str => <Item>{"mirror/" + str}</Item>))
    object rt1 extends RuleTransformer(addExcludeItems)

    object tr extends RewriteRule {
      override def transform(n: Node): collection.Seq[Node] = n match {
        case sn @ Elem (_, "FolderPairs", _, _, _*) => rt1(sn)
        case other => other
      }
    }

    object rt2 extends RuleTransformer(tr)
    val newXml = rt2(xmlFile)
    val prettyPrinter = new PrettyPrinter(90,2)
    val fileWriter = new FileWriter(path, false);
    try {
      fileWriter.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" + prettyPrinter.format(newXml))
    } finally fileWriter.close()

  }


  def computeHash(file: File, messageDigest: MessageDigest): String = {
    val buffer = new Array[Byte](8192)
    val digestInputStream = new DigestInputStream(new FileInputStream(file), messageDigest)

    try {while (digestInputStream.read(buffer) != -1) {} } finally { digestInputStream.close() }

    messageDigest.digest().map("%02x".format(_)).mkString
  }
}

class UniqueFile(val file : File, val sha1: String, val copies: Array[File]) {
  override def toString: String = {
    s"{ file: ${file.getName}, sha1: $sha1, copies: ${copies.map(_.getName).mkString("[", ", ", "] }")}"
  }
}

class AddExcludeItems(items: Iterable[Node]) extends RewriteRule {

  override def transform(n: Node): Node = n match {
    case n @ Elem(prefix, "Exclude", attr, scope, child @ _*) => Elem(prefix, "Exclude", attr, scope, child.appendedAll(items) : _*)
    case other => other
  }
}



/*

*/
/*
    val uniqueFiles = filesByHash.map( (kv: (String, Array[File])) =>
      kv match {
        case (hash, files) =>  val sorted = files.sortBy(_.getName)
          val filesToDelete = sorted.drop(1)
          new UniqueFile(sorted(0), hash, filesToDelete)
      }
    )
    */

