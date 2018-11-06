object Test {
  def main(args:Array[String]) = {
    val array = Array("duration", "protocol_type_Vec", "service_Vec", "flag_Vec", "src_bytes", "dst_bytes", "land_Vec", "wrong_fragment", "urgent", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate", "dst_host_srv_rerror_rate")
    array.foreach(x => {
      println(x)
    })
  }
}
