package com.example.barefoot_spark

import java.net.URI
import java.util.{ArrayList, List => JavaList}

import scala.collection.JavaConverters._

import com.bmwcarit.barefoot.matcher.{Matcher, MatcherKState, MatcherSample}
import com.bmwcarit.barefoot.roadmap.{Road, RoadMap, RoadPoint, TimePriority}
import com.bmwcarit.barefoot.spatial.Geography
import com.bmwcarit.barefoot.topology.Dijkstra
import com.example.barefoot_spark.HadoopMapReader

object BroadcastMatcher {
  private var instance = null: Matcher

  private def initialize(map_uri: String) {
    this.synchronized {
      if (instance == null) {
        val map = RoadMap.Load(new HadoopMapReader(new URI(map_uri))).construct()
        val router = new Dijkstra[Road, RoadPoint]()
        val cost = new TimePriority()
        val spatial = new Geography()

        instance = new Matcher(map, router, cost, spatial)
      }
    }
  }
}

@SerialVersionUID(1L)
class BroadcastMatcher(map_uri: String) extends Serializable {

  def mmatch(samples: JavaList[MatcherSample]): MatcherKState = {
    mmatch(samples, 0, 0)
  }

  def mmatch(samples: JavaList[MatcherSample], minDistance: Double, minInterval: Int): MatcherKState = {
    
    val mutableSamples = new ArrayList[MatcherSample](samples)

    BroadcastMatcher.initialize(map_uri)
    BroadcastMatcher.instance.mmatch(mutableSamples, minDistance, minInterval)
  }
}