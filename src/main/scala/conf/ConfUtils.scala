package conf

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object ConfUtils {
  private val logger:Logger = LoggerFactory.getLogger(ConfUtils.getClass)
  def loadPropertiesToMap(fileName:String):Map[String, String] = {
    val properties = loadpropertiesFile(fileName)
    properties.stringPropertyNames().asScala.map{
      x => (x, properties.getProperty(x))
    }.toMap[String, String]
  }

  def loadpropertiesFile(fileName:String) = {
    val inputstream = ConfUtils.getClass.getClassLoader.getResourceAsStream(fileName)
    val properties = new Properties()
    try {
      properties.load(inputstream)
    } catch {
      case e:Exception => {
        logger.error("loadProperties failed, fileName = {}", fileName)
        throw new Exception("loadPropertiesFile failed!")
      }
    } finally {
      inputstream.close()
    }
    properties
  }
}
