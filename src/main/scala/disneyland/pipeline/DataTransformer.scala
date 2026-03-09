package disneyland.pipeline

import scala.util.Try // ใช้ Try เพื่อป้องกัน error ตอนแปลงข้อมูล

object DataTransformer:
  // แปลงคะแนนดาวเป็นเปอร์เซ็นต์ (สมมติว่าคะแนนเต็มคือ 5 ดาว)
  def transformRating(ratingStr: String): Option[Double] = 
    Try((ratingStr.toDouble / 5.0) * 100).toOption // แปลงคะแนนดาวเป็นเปอร์เซ็นต์ (0–100) และคืนค่าเป็น Option หากแปลงไม่ได้จะได้ None

  // แยกปีและเดือน จัดการกรณีที่เป็นคำว่า "missing"
  def transformYearMonth(ymStr: String): (Option[Int], Option[Int]) = 
    if ymStr.toLowerCase == "missing" then (None, None) // กรณีข้อมูลเป็น missing ให้คืนค่า None ทั้งปีและเดือน
    else 
      val parts = ymStr.split("-") // แยกข้อมูลปีและเดือนจากรูปแบบ "YYYY-MM"
      val year = Try(parts(0).toInt).toOption // แปลงปีเป็น Int ถ้าแปลงไม่ได้จะเป็น None
      val month = Try(parts(1).toInt).toOption // แปลงเดือนเป็น Int ถ้าแปลงไม่ได้จะเป็น None
      (year, month) // คืนค่าปีและเดือนในรูปแบบ Option

  // แปลงประเทศเป็นทวีป
  def transformLocation(country: String): String = country.toLowerCase match { // แปลงชื่อประเทศเป็นทวีป
    case c if c.contains("united states") || c.contains("canada") => "North America" // ประเทศในอเมริกาเหนือ
    case c if c.contains("united kingdom") || c.contains("france") || c.contains("germany") => "Europe" // ประเทศในยุโรป
    case c if c.contains("australia") || c.contains("new zealand") => "Oceania" // ประเทศในโอเชียเนีย
    case c if c.contains("philippines") || c.contains("hong kong") || c.contains("japan") || c.contains("singapore") => "Asia" // ประเทศในเอเชีย
    case _ => "Other/Unknown" // กรณีไม่ตรงเงื่อนไขใด
  }
  // แปลงชื่อสาขาเป็น ID สั้นๆ
  def transformBranch(branch: String): String = branch match { // แปลงชื่อสาขา Disneyland เป็นรหัสสั้น
    case "Disneyland_HongKong" => "HKG" // สาขาฮ่องกง
    case "Disneyland_California" => "CAL" // สาขาแคลิฟอร์เนีย
    case "Disneyland_Paris" => "PAR" // สาขาปารีส
    case _ => "UNKNOWN" // กรณีไม่พบสาขา
  }
