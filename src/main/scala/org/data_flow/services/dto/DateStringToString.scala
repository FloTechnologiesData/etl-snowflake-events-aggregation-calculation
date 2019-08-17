package org.data_flow.services.dto

import java.text.SimpleDateFormat
import java.util.Calendar
import org.data_flow.constants.Constants

/**
  * Utility Class to subtract one second from given start date
  */
class DateStringToString extends Constants {

  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * subtract one second from given date string
    * @param dateStr
    * @return
    */
  def dateStrToStr(dateStr: String): String ={

    val cal = Calendar.getInstance
    cal.setTime(formatter.parse(dateStr))
    cal.add(Calendar.SECOND, -1)
    val month = if(cal.get(Calendar.MONTH)+1 < 10) 0+""+(cal.get(Calendar.MONTH)+1) else cal.get(Calendar.MONTH)+1
    val hour = if(cal.get(Calendar.HOUR_OF_DAY) < 10) 0+""+cal.get(Calendar.HOUR_OF_DAY) else cal.get(Calendar.HOUR_OF_DAY)
    val minute = if(cal.get(Calendar.MINUTE) < 10) 0+""+cal.get(Calendar.MINUTE) else cal.get(Calendar.MINUTE)
    val seconds = if(cal.get(Calendar.SECOND) < 10) 0+""+cal.get(Calendar.SECOND) else cal.get(Calendar.SECOND)
    val date = if (cal.get(Calendar.DATE) < 10) 0 + "" + cal.get(Calendar.DATE) else cal.get(Calendar.DATE)
    cal.get(Calendar.YEAR) + "-" + month + "-" + date + " " + hour + ":" +
      minute +":"+seconds

  }

  /**
    * Add One second to given date String
    * @param dateStr
    * @return
    */
  def addOneSecond(dateStr: String): String = {
    val cal = Calendar.getInstance
    cal.setTime(formatter.parse(dateStr))
    cal.add(Calendar.SECOND, 1)
    val month = if(cal.get(Calendar.MONTH)+1 < 10) 0+""+(cal.get(Calendar.MONTH)+1) else cal.get(Calendar.MONTH)+1
    val hour = if(cal.get(Calendar.HOUR_OF_DAY) < 10) 0+""+cal.get(Calendar.HOUR_OF_DAY) else cal.get(Calendar.HOUR_OF_DAY)
    val minute = if(cal.get(Calendar.MINUTE) < 10) 0+""+cal.get(Calendar.MINUTE) else cal.get(Calendar.MINUTE)
    val seconds = if(cal.get(Calendar.SECOND) < 10) 0+""+cal.get(Calendar.SECOND) else cal.get(Calendar.SECOND)
    val date = if (cal.get(Calendar.DATE) < 10) 0 + "" + cal.get(Calendar.DATE) else cal.get(Calendar.DATE)
    cal.get(Calendar.YEAR) + "-" + month + "-" + date + " " + hour + ":" +
      minute +":"+seconds
  }

  /**
    * Add One second to given date String
    * @param dateStr
    * @return
    */
  def addOneDay(dateStr: String): String = {
    val cal = Calendar.getInstance
    cal.setTime(formatter.parse(dateStr))
    cal.add(Calendar.DATE, 1)
    val month = if(cal.get(Calendar.MONTH)+1 < 10) 0+""+(cal.get(Calendar.MONTH)+1) else cal.get(Calendar.MONTH)+1
    val date = if (cal.get(Calendar.DATE) < 10) 0 + "" + cal.get(Calendar.DATE) else cal.get(Calendar.DATE)
    cal.get(Calendar.YEAR) + "-" + month + "-" + date + " "
  }

}
