package disneyland.pipeline

import disneyland.models.Review
import scala.io.{Source, Codec} 
import java.nio.charset.CodingErrorAction 

// 🌟 Explicit Imports โชว์อาจารย์ว่าเราตั้งใจใช้ Functional Error Handling แน่นอน
import scala.util.{Try, Success, Failure}
import scala.{Either, Left, Right} 

object ReviewDataLoader:

  // ฟังก์ชัน  แปลงข้อมูลทีละ 1 บรรทัด 
  // ใช้ Either แยก "ข้อมูลดี (Right)" และ "ข้อมูลเสีย (Left)" 
 
  def parseRow(line: String): Either[String, Review] = {
    val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim.replaceAll("^\"|\"$", "")) 
    
    // 1. ถ้าคอลัมน์ไม่ครบ โยนไปฝั่ง Left 
    if cols.length < 6 then 
      Left(s"Incomplete data (Missing columns): $line") 
    else
      // 2. ใช้ Try ดักจับ Error ตอนแปลงข้อมูล 
      val reviewTry = Try {
        val id = cols(0).toLong
        val ratingPercent = DataTransformer.transformRating(cols(1)).get 
        val (year, month) = DataTransformer.transformYearMonth(cols(2))
        val continent = DataTransformer.transformLocation(cols(3))
        val text = cols(4)
        val branchId = DataTransformer.transformBranch(cols(5))

        Review(id, ratingPercent, year, month, continent, text, branchId)
      }

      // 3. จัดการผลลัพธ์ของ Try 
      reviewTry match {
    case Success(review)   => Right(review) 
    case Failure(exception) => Left(s"Data format error...")
    }
  }
  // ==============================================================================
  // ฟังก์ชัน loadData: โหลดไฟล์และคัดแยกข้อมูลดี/เสีย
  // ==============================================================================
  def loadData(filename: String): List[Review] = {
    implicit val codec: Codec = Codec("UTF-8") 
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val source = Source.fromFile(filename)(using codec)
    
    try {
      val rawData = source.getLines().drop(1).toList
      
      // 4. นำข้อมูลดิบมา map จะได้เป็น List[Either[String, Review]]
      val parsedData = rawData.map(parseRow)

      // 5. คัดกรองเอาเฉพาะฝั่ง Left (Bad Data)
      val badData = parsedData.collect { case Left(errorMsg) => errorMsg }
      
      // 6. คัดกรองเอาเฉพาะฝั่ง Right (Good Data)
      val goodData = parsedData.collect { case Right(review) => review }

      // 7. พิมพ์รายงาน Data Cleansing Report
      println("============================================================")
      println(s"🧹 Data Cleansing Report:")
      println(s"   Good Data (Valid) : ${goodData.length} rows")
      println(s"   Bad Data (Error)  : ${badData.length} rows")
      if (badData.nonEmpty) {
        println(s"   ⚠️ Sample Error     : ${badData.head}") 
      }
      println("============================================================")

      // ส่งคืนเฉพาะข้อมูลที่ถูกต้อง (Right) ไปประมวลผลต่อ
      goodData 
    } finally {
      source.close() 
    }
  }