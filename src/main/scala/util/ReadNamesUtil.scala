package util

import java.io.{BufferedReader, InputStreamReader}

import org.slf4j.LoggerFactory

import scala.collection.mutable

object ReadNamesUtil {
   def getNamesArray(fileName:String):Array[String] = {
     val logger = LoggerFactory.getLogger(ReadNamesUtil.getClass)

     val ins = ReadNamesUtil.getClass.getClassLoader.getResourceAsStream(fileName)
     val reader = new BufferedReader(new InputStreamReader(ins))
     var list = mutable.MutableList.empty[String]
     var str:String = null;
     try {
       str = reader.readLine()
       while (str != null) {
         list += str
         str = reader.readLine()
       }
     } catch {
       case e: Exception=> {
         logger.error(e.getMessage)
         logger.error("read {} occur errors!", {})
       }
     }
     list.toArray[String]
   }
}
