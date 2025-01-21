package com.example.barefoot_spark

import java.lang.{Short => JShort}

import java.io.ObjectInput
import java.io.ObjectInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URI
import java.util.{HashSet => JHashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry.Polygon
import com.esri.core.geometry.SpatialReference

import com.bmwcarit.barefoot.road.BaseRoad
import com.bmwcarit.barefoot.road.RoadReader
import com.bmwcarit.barefoot.util.SourceException


class HadoopMapReader(uri: URI) extends RoadReader {

  private var reader : Option[ObjectInput] = None
  private var exclusions : JHashSet[JShort] = null
  private var polygon : Polygon = null;

  override def isOpen() : Boolean = reader.nonEmpty

  override def open() = {
    open(null, null)
  }

  override def open(polygon : Polygon, exclusions : JHashSet[JShort]) : Unit = {
    try {
      val conf = new Configuration()
      val fs = FileSystem.get(uri, conf)
      val in = fs.open(new Path(uri))
      this.reader = Some(new ObjectInputStream(in))
      this.exclusions = exclusions
      this.polygon = polygon
    } catch {
      case e: FileNotFoundException =>
        throw new SourceException("File could not be found for uri: " + uri)
      case e: IOException =>
        throw new SourceException("Opening reader failed")
    }
  }

  override def close() = {
    try {
      reader map { _.close }
      reader = None
    } catch {
      case e: IOException =>
        throw new SourceException("Closing file failed.")
    }
  }

  override def next() : BaseRoad = {
    if (!isOpen()) {
      throw new SourceException("File is closed or invalid.")
    }

    reader match {
      case Some(r) =>
        try {
          var road : BaseRoad = null
          do {
            road = r.readObject().asInstanceOf[BaseRoad]
            if (road == null) {
              return null
            }
          } while (
              exclusions != null && exclusions.contains(road.`type`())
            ||
              polygon != null
              && !GeometryEngine.contains(polygon, road.geometry(), SpatialReference.create(4326))
              && !GeometryEngine.overlaps(polygon, road.geometry(), SpatialReference.create(4326))
          )
          road
        } catch {
          case e: ClassNotFoundException =>
            throw new SourceException("File is corrupted, read object is not a road.")
          case e: IOException =>
            throw new SourceException("Reading file failed")
        }
      case None =>
        throw new SourceException("File not open")
    }
  }
}