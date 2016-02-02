package com.sumologic.elasticsearch.util
import scala.collection.mutable
/**
 * Created by russell on 1/29/16.
 * Track timing for multiple named events
 */
class InstrumentationContext {
  private val resultMap = mutable.Map[String, Long]().withDefaultValue(0L)
  def measureAction[T](action: String)(fn: => T): T = {
    val startTime = System.currentTimeMillis()
    val res = fn
    val endTime = System.currentTimeMillis()
    resultMap.put(action, endTime - startTime + resultMap(action))
    res
  }

  def measurementsString: String = {
    resultMap.map({
      case (actionName, measurement) => s"$actionName='${measurement}ms'"
    }).mkString(", ")
  }

}
