class Utilities {
  def  findNumberOfCommonWords(from:Seq[String], to:Seq[String])={
    if(from == null || to == null) 0
    else from.intersect(to).length
  }

  def  findNumberOfCommonNeighbours(from:Seq[String], to:Seq[String])={
    if(from == null || to == null) 0
    else from.intersect(to).length
  }
}
