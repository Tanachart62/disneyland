package disneyland

import disneyland.pipeline.{ReviewDataLoader, ReviewAnalytics}
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

@main def runDisneylandETL(): Unit = {
  
  println("Loading Dataset...")
  val dataset = ReviewDataLoader.loadData("data/DisneylandReviews.csv") 
  println(s"Loaded ${dataset.length} valid reviews (Bad data filtered out).\n")

  // ---------------------------------------------------------
  // แบบที่ 1: Sequential Processing (ประมวลผลทีละคำสั่ง)
  // ---------------------------------------------------------
  val startTime1 = System.currentTimeMillis()
  val avgSeq = ReviewAnalytics.averageRatingByBranch(dataset)
  val countSeq = ReviewAnalytics.countByContinent(dataset)
  val wordsSeq = ReviewAnalytics.findTopWords(dataset)
  val timeSeq = System.currentTimeMillis() - startTime1
  println(s"[1] Sequential Time Spent: ${timeSeq} ms")

  // ---------------------------------------------------------
  // แบบที่ 2: Task-Level Parallelism (ใช้ Future แยก 3 งานไปทำพร้อมกัน)
  // ---------------------------------------------------------
  val startTime2 = System.currentTimeMillis()
  val futureAvg = Future { ReviewAnalytics.averageRatingByBranch(dataset) }
  val futureCount = Future { ReviewAnalytics.countByContinent(dataset) }
  val futureWords = Future { ReviewAnalytics.findTopWords(dataset) }

  val combinedFuture = for { a <- futureAvg; c <- futureCount; w <- futureWords } yield (a, c, w)
  val (aResult2, cResult2, wResult2) = Await.result(combinedFuture, 20.seconds)
  val timeFuture = System.currentTimeMillis() - startTime2
  println(s"[2] Future (Task-Level Parallel) Time Spent: ${timeFuture} ms")

  // ---------------------------------------------------------
  // แบบที่ 3: Data-Level Parallelism (ใช้ .par หั่นข้อมูลไปให้ CPU หลาย Core ทำพร้อมกัน)
  // ---------------------------------------------------------
  val startTime3 = System.currentTimeMillis()
  val avgPar = ReviewAnalytics.averageRatingByBranchPar(dataset)
  val countPar = ReviewAnalytics.countByContinentPar(dataset)
  val wordsPar = ReviewAnalytics.findTopWordsPar(dataset)
  val timePar = System.currentTimeMillis() - startTime3
  println(s"[3] Parallel Collections (.par) Time Spent: ${timePar} ms")

  println("------------------------------------------------------------")
  println(s"1. Average by branch (%): \n   $avgPar")
  println(s"2. Number of reviews separated by continent: \n   $countPar")
  println("============================================================")
}