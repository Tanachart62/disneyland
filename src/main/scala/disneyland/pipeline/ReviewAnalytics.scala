package disneyland.pipeline

import disneyland.models.Review
import scala.collection.parallel.CollectionConverters._

object ReviewAnalytics:
  // =========================================================
  // 1. แบบปกติ (Sequential Processing)
  // =========================================================
  def averageRatingByBranch(reviews: List[Review]): Map[String, Double] = {
    reviews.groupBy(_.branchId).map { case (branch, list) =>
      val avg = list.map(_.ratingPercent).sum[Double] / list.size // ระบุ [Double] ให้ชัดเจนแก้บั๊ก Scala 3
      (branch, avg)
    }
  }

  def countByContinent(reviews: List[Review]): Map[String, Int] = {
    reviews.groupBy(_.continent).map { case (cont, list) => (cont, list.size) }
  }

  def findTopWords(reviews: List[Review]): List[(String, Int)] = {
    reviews.flatMap(r => r.reviewText.toLowerCase.split("\\W+"))
           .filter(w => w.length > 4) // ระบุ w => เพื่อช่วย Compiler
           .filterNot(w => w == "disneyland" || w == "park" || w == "ride" || w == "there" || w == "their")
           .groupBy(identity)
           .map { case (word, list) => (word, list.length) }
           .toList.sortBy(-_._2).take(5)
  }

  // =========================================================
  // 2. แบบประมวลผลขนานระดับข้อมูล (Data-Level Parallelism ใช้ .par)
  // =========================================================
  def averageRatingByBranchPar(reviews: List[Review]): Map[String, Double] = {
    reviews.par.groupBy(_.branchId).map { case (branch, list) =>
      val avg = list.map(_.ratingPercent).sum[Double] / list.size
      (branch, avg)
    }.seq.toMap
  }

  def countByContinentPar(reviews: List[Review]): Map[String, Int] = {
    reviews.par.groupBy(_.continent).map { case (cont, list) => 
      (cont, list.size) 
    }.seq.toMap
  }

  def findTopWordsPar(reviews: List[Review]): List[(String, Int)] = {
    reviews.par.flatMap(r => r.reviewText.toLowerCase.split("\\W+"))
           .filter(w => w.length > 4)
           .filterNot(w => w == "disneyland" || w == "park" || w == "ride" || w == "there" || w == "their")
           .groupBy(identity)
           .map { case (word, list) => (word, list.size) } 
           .toList.sortBy(-_._2).take(5)
  }