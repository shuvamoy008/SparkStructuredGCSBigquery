package org.apache.spark.examples.Utils

import java.io.{File, FileOutputStream, InputStream, OutputStream}

object readFileFromResource extends Serializable {

  def readFromResource(input: String): File= {
    var source: Option[InputStream] = None
    var out: Option[OutputStream] = None

    // val resourcePath = "/source_hierarchy.yaml"
    try {
      source = Option(getClass.getResourceAsStream(input))
      val tempFile = File.createTempFile(String.valueOf(source.hashCode()), ".avsc")
      out = Some(new FileOutputStream(tempFile))
      val buffer = new Array[Byte](2048)
      var length = -1;
      do {
        length = source.get.read(buffer)
        if (length != -1)
          out.get.write(buffer, 0, length)
      } while (length != -1)
      tempFile
      // Some(source)
    } finally {
      source.foreach(_.close)
      out.foreach(_.close)

    }
  }
}
