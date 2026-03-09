package disneyland

import disneyland.pipeline.{ReviewDataLoader, ReviewAnalytics}  //ดึงฟังก์ชันจาก review data loader และ analytics มาใช้
import scala.concurrent.{Future, Await} // ใช้ Future เพื่อทำงานแบบขนาน และ Await เพื่อรอผลลัพธ์ของ Future
import scala.concurrent.ExecutionContext.Implicits.global  // ตัวจัดการคิวงานของซีพียู เพื่อให้ Future ทำงานแบบขนาน
import scala.concurrent.duration._  // ใช้สำหรับกำหนดเวลารอผลลัพธ์ของ Future

// Import สำหรับจัดการ JSON และการเขียนไฟล์
import upickle.default.*
import java.io.PrintWriter

@main def runDisneylandETL(): Unit = {

  println("Loading Dataset...")
  val dataset = ReviewDataLoader.loadData("data/DisneylandReviews.csv")  //เรียกใช้ฟังก์ชัน loadData เพื่อโหลดข้อมูลจากไฟล์ CSV และแปลงเป็น List[Review]
  println(s"Loaded ${dataset.length} valid reviews (Bad data filtered out).\n")   

  // ---------------------------------------------------------
  // แบบที่ 1: Sequential Processing 
  // ---------------------------------------------------------
  val startTime1 = System.currentTimeMillis()   // เริ่มจับเวลา
  val avgSeq = ReviewAnalytics.averageRatingByBranch(dataset)   // คำนวณค่าเฉลี่ย
  val countSeq = ReviewAnalytics.countByContinent(dataset) //นับจำนวนรีวิวแยกตามทวีป
  val wordsSeq = ReviewAnalytics.findTopWords(dataset)  //หาคำที่พบบ่อยในรีวิว
  val timeSeq = System.currentTimeMillis() - startTime1 //เอาเวลาปัจจุบันลบกับเวลาที่เริ่ม
  println(s"[1] Sequential Time Spent: ${timeSeq} ms") //เเสดงผลเวลา

  // ---------------------------------------------------------
  // แบบที่ 2: Task-Level Parallelism (Future)
  // ---------------------------------------------------------
  val startTime2 = System.currentTimeMillis() //เริ่มจับเวลา
  val futureAvg = Future { ReviewAnalytics.averageRatingByBranch(dataset) } 
  val futureCount = Future { ReviewAnalytics.countByContinent(dataset) }  //ประมวณผลแบบขนานโดยใช้ Future เพื่อให้แต่ละงานทำงานพร้อมกัน
  val futureWords = Future { ReviewAnalytics.findTopWords(dataset) } 
  
  val combinedFuture = for { a <- futureAvg; c <- futureCount; w <- futureWords } yield (a, c, w) //มัดรวมทั้ง3เป็นฟังก์ชันเดียวกัน
  val (aResult2, cResult2, wResult2) = Await.result(combinedFuture, 20.seconds) //สั่งให้ต้องรอจนกว่าจะเสร็จทุกฟังชั่น 
  val timeFuture = System.currentTimeMillis() - startTime2 //เอาเวลาปัจจุบันลบกับเวลาที่เริ่ม
  println(s"[2] Future (Task-Level Parallel) Time Spent: ${timeFuture} ms") 

  // ---------------------------------------------------------
  // แบบที่ 3: Data-Level Parallelism (Parallel Collection)
  // ---------------------------------------------------------
  val startTime3 = System.currentTimeMillis() 
  val avgPar = ReviewAnalytics.averageRatingByBranchPar(dataset) 
  val countPar = ReviewAnalytics.countByContinentPar(dataset) 
  val wordsPar = ReviewAnalytics.findTopWordsPar(dataset) //เริ่มจับเวลาและหั่นข้อมูลไปประมวณผลในแต่ละ core ของซีพียูเพื่อให้ทำงานแบบขนาน
  val timePar = System.currentTimeMillis() - startTime3 
  println(s"[3] Parallel Collections (.par) Time Spent: ${timePar} ms") 

  // เปรียบเทียบว่า execution mode ไหนเร็วที่สุด
  val fastest =
    if timeSeq <= timeFuture && timeSeq <= timePar then "Sequential"
    else if timeFuture <= timePar then "Future Parallel"
    else "Parallel Collection"

  println(s"Fastest execution mode: $fastest") 

  println("------------------------------------------------------------")
  println(s"1. Average by branch (%): \n   $avgPar") 
  println("------------------------------------------------------------")
  println(s"2. Number of reviews separated by continent: \n   $countPar") 
  println("------------------------------------------------------------")

  // ---------------------------------------------------------
  // Analytics Result
  // ---------------------------------------------------------
  println("3. Top words appearing in reviews:") 
  wordsPar.foreach { case (word, count) =>
    println(s"   $word -> $count")
  }
  println("============================================================")

  // ---------------------------------------------------------
  // Export ไฟล์ที่ 1: Analysis Result (ผลวิเคราะห์)
  // ---------------------------------------------------------
  val resultJson = ujson.Obj(
    "performance" -> ujson.Obj(
      "sequential_ms" -> timeSeq,
      "future_ms" -> timeFuture,
      "parallel_collection_ms" -> timePar,
      "fastest" -> fastest
    ),
    "analysis" -> ujson.Obj(
      "average_by_branch" -> avgPar,
      "reviews_by_continent" -> countPar,
      "top_words" -> wordsPar.toMap 
    )
  ) //นำข้อมูลผลวิเคราะห์แล้ว มาแปลงเป็น json object เพื่อเตรียมส่งออกเป็นไฟล์ json

  val analysisJsonString = write(resultJson, indent = 2) //แปลง JSON Object เป็น String 
  val analysisWriter = new PrintWriter("data/analysis_output.json") //สร้าง PrintWriter เพื่อเขียนไฟล์ผลวิเคราะห์ออกมาเป็นไฟล์ json ลงในโฟลเดอร์ data
  analysisWriter.write(analysisJsonString) //เขียนข้อมูล JSON String ลงในไฟล์
  analysisWriter.close() //ปิด PrintWriter หลังจากเขียนเสร็จ
  println("1. Analysis result exported to data/analysis_output.json")

  // ---------------------------------------------------------
  // Export ไฟล์ที่ 2: Transformed Data (แปลงข้อมูลดิบจาก CSV เป็น JSON)
  // ---------------------------------------------------------
  // แปลง List[Review] เป็น JSON Array
  val transformedDataArray = ujson.Arr.from(dataset.map { r =>
    ujson.Obj(
      "id" -> r.id.toDouble,
      "ratingPercent" -> r.ratingPercent,
      // ถ้าไม่มีปี/เดือน (เป็น None) ให้ใส่ค่า null ใน JSON
      "year" -> r.year.map(y => ujson.Num(y.toDouble)).getOrElse(ujson.Null),
      "month" -> r.month.map(m => ujson.Num(m.toDouble)).getOrElse(ujson.Null),
      "continent" -> r.continent,
      "reviewText" -> r.reviewText,
      "branchId" -> r.branchId
    )
  })//นำข้อมูลดิบที่เป็น List[Review] มาแปลงเป็น JSON Array โดยใช้ map เพื่อแปลงแต่ละรีวิวเป็น JSON Object และจัดการกับค่า None ให้เป็น null ใน JSON

  
  val dataJsonString = write(transformedDataArray, indent = 2)
  val dataWriter = new PrintWriter("data/transformed_data.json")//สร้าง PrintWriter เพื่อเขียนไฟล์ข้อมูลที่แปลงแล้วออกมาเป็นไฟล์ json ลงในโฟลเดอร์ data
  dataWriter.write(dataJsonString)
  dataWriter.close()
  println("2. Transformed data exported to data/transformed_data.json")
  println("============================================================")
}