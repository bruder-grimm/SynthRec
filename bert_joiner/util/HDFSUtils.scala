package plista.ml.util

import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtils {

  /** Returns the latest (highest number) partition/folder at the specified path */
  def getLatestPartition(path: String)(implicit fs: FileSystem): String =
    fs.listStatus(new Path(path))
      .filter(_.isDirectory)
      .maxBy(_.getPath.getName)
      .getPath
      .toString

  case class UserHistory(user: Seq[Long], history: Seq[Long])
}
