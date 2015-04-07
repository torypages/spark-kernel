package com.ibm.spark.utils

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._


object MemoryMonitor {


  def lowMemory(percent : Double  = 0.05 ): Boolean = {
    var beans  =  ManagementFactory.getMemoryPoolMXBeans().asScala

   beans.exists( item => {
      val usage = item.getUsage

      if ((usage.getMax - usage.getUsed) < percent * usage.getMax)
      {
        if (item.getName  != "PS Survivor Space")
          return true
      }
      false
    })
  }
}