package conf

import org.slf4j.{Logger, LoggerFactory}

object Config {
  private val logger:Logger = LoggerFactory.getLogger(Config.getClass)
  private val configPath = "config.properties"
  private val properties = ConfUtils.loadPropertiesToMap(configPath)

  def getString(key:String):String = {
    properties.get(key) match {
      case Some(s) => s
      case None => null
    }
  }

  def getInt(key:String):Int = {
     val s = getString(key)
     s.toInt
  }

  def getFloat(key:String):Float = {
    val s = getString(key)
    s.toFloat
  }
}
