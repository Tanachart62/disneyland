package disneyland.pipeline

import scala.util.Try

object DataTransformer:
  // แปลงคะแนนดาวเป็นเปอร์เซ็นต์ (สมมติว่าคะแนนเต็มคือ 5 ดาว)
  def transformRating(ratingStr: String): Option[Double] = 
    Try((ratingStr.toDouble / 5.0) * 100).toOption

  // แยกปีและเดือน จัดการกรณีที่เป็นคำว่า "missing"
  def transformYearMonth(ymStr: String): (Option[Int], Option[Int]) = 
    if ymStr.toLowerCase == "missing" then (None, None)
    else 
      val parts = ymStr.split("-")
      val year = Try(parts(0).toInt).toOption
      val month = Try(parts(1).toInt).toOption
      (year, month)

  // แปลงประเทศเป็นทวีป
  def transformLocation(country: String): String = country.toLowerCase match {
    case c if c.contains("united states") || c.contains("canada") => "North America"
    case c if c.contains("united kingdom") || c.contains("france") || c.contains("germany") => "Europe"
    case c if c.contains("australia") || c.contains("new zealand") => "Oceania"
    case c if c.contains("philippines") || c.contains("hong kong") || c.contains("japan") || c.contains("singapore") => "Asia"
    case _ => "Other/Unknown"
  }

  // แปลงชื่อสาขาเป็น ID สั้นๆ
  def transformBranch(branch: String): String = branch match {
    case "Disneyland_HongKong" => "HKG"
    case "Disneyland_California" => "CAL"
    case "Disneyland_Paris" => "PAR"
    case _ => "UNKNOWN"
  }