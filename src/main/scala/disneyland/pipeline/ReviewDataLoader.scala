package disneyland.pipeline

import disneyland.models.Review
import scala.io.{Source, Codec} // ใช้อ่านไฟล์และกำหนด encoding
import java.nio.charset.CodingErrorAction // ใช้กำหนดวิธีจัดการตัวอักษรที่ decode ไม่ได้

// Explicit Imports สำหรับ Functional Error Handling
import scala.util.{Try, Success, Failure} // ใช้ Try จัดการ error ตอนแปลงข้อมูล
import scala.{Either, Left, Right} // ใช้ Either แยกข้อมูลดี (Right) และข้อมูลเสีย (Left)

object ReviewDataLoader:

  // ฟังก์ชันแปลงข้อมูลทีละ 1 บรรทัด
  // คืนค่า Either เพื่อแยก Good Data / Bad Data
  def parseRow(line: String): Either[String, Review] = {

    // แยกข้อมูล CSV โดยรองรับกรณีที่มีเครื่องหมาย quote
    val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim.replaceAll("^\"|\"$", "")) 
    
    // ถ้าคอลัมน์ไม่ครบให้ถือเป็นข้อมูลเสีย
    if cols.length < 6 then 
      Left(s"Incomplete data (Missing columns): $line") 
    else

      // ใช้ Try ป้องกัน error ตอนแปลงข้อมูล
      val reviewTry = Try {
        val id = cols(0).toLong // แปลง id เป็น Long
        val ratingPercent = DataTransformer.transformRating(cols(1)).get // แปลง rating เป็นเปอร์เซ็นต์
        val (year, month) = DataTransformer.transformYearMonth(cols(2)) // แยกปีและเดือน
        val continent = DataTransformer.transformLocation(cols(3)) // แปลงประเทศเป็นทวีป
        val text = cols(4) // ข้อความรีวิว
        val branchId = DataTransformer.transformBranch(cols(5)) // แปลงชื่อสาขาเป็น ID

        Review(id, ratingPercent, year, month, continent, text, branchId) // สร้าง Review object
      }

      // จัดการผลลัพธ์ของ Try
      reviewTry match {
        case Success(review)   => Right(review) // ข้อมูลถูกต้อง
        case Failure(exception) => Left(s"Data format error...") // ข้อมูลผิดรูปแบบ
      }
  }

  // ==============================================================================
  // ฟังก์ชัน loadData: โหลดไฟล์และคัดแยกข้อมูลดี/เสีย
  // ==============================================================================
  def loadData(filename: String): List[Review] = {

    // กำหนด encoding ของไฟล์เป็น UTF-8
    implicit val codec: Codec = Codec("UTF-8") 

    // หากเจอ character ที่อ่านไม่ได้ให้แทนที่แทนการ error
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // เปิดไฟล์อ่านข้อมูล
    val source = Source.fromFile(filename)(using codec)
    
    try {

      // อ่านไฟล์ทั้งหมดและข้าม header (บรรทัดแรก)
      val rawData = source.getLines().drop(1).toList
      
      // แปลงข้อมูลแต่ละบรรทัด → ได้ List[Either[String, Review]]
      val parsedData = rawData.map(parseRow)

      // เก็บเฉพาะข้อมูลที่ผิดพลาด (Left)
      val badData = parsedData.collect { case Left(errorMsg) => errorMsg }
      
      // เก็บเฉพาะข้อมูลที่ถูกต้อง (Right)
      val goodData = parsedData.collect { case Right(review) => review }

      // แสดงรายงาน Data Cleansing
      println("============================================================")
      println(s"Data Cleansing Report:")
      println(s"Good Data (Valid) : ${goodData.length} rows")
      println(s"Bad Data (Error)  : ${badData.length} rows")

      // แสดงตัวอย่าง error ถ้ามี
      if (badData.nonEmpty) {
        println(s"Sample Error : ${badData.head}") 
      }

      println("============================================================")

      // ส่งคืนเฉพาะข้อมูลที่ถูกต้อง
      goodData 

    } finally {
      source.close() // ปิดไฟล์หลังใช้งาน
    }
  }
