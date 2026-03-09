package disneyland.pipeline

import disneyland.models.Review
import scala.collection.parallel.CollectionConverters._

object ReviewAnalytics:
  private val stopWords = Set("disneyland", "park", "ride", "there", "their")  // กำหนดชุดคำที่ไม่มีนัยสำคัญ (Stop Words) ไว้ในโครงสร้างข้อมูลแบบ Set

  // =========================================================
  // 1. การประมวลผลแบบลำดับ (Sequential Processing)
  // =========================================================
  
  def averageRatingByBranch(reviews: List[Review]): Map[String, Double] = {
    // จัดกลุ่มข้อมูลรีวิวตามรหัสสาขา (branchId)
    reviews.groupBy(_.branchId).map { case (branch, list) =>
      // คำนวณหาค่าเฉลี่ย โดยระบุ Type [Double] ให้กับเมธอด .sum อย่างชัดเจน 
      val avg = list.map(_.ratingPercent).sum[Double] / list.size 
      (branch, avg)
    }
  }

  def countByContinent(reviews: List[Review]): Map[String, Int] = {
    // จัดกลุ่มข้อมูลตามทวีป และแมปค่า Value ให้เป็นจำนวนสมาชิก (.size) ของแต่ละกลุ่ม
    reviews.groupBy(_.continent).map { case (cont, list) => (cont, list.size) }
  }

  def findTopWords(reviews: List[Review]): List[(String, Int)] = {
    reviews.flatMap(r => r.reviewText.toLowerCase.split("\\W+"))  // 1. แปลงข้อความทั้งหมดเป็นตัวพิมพ์เล็ก และแยกคำด้วย Regular Expression (\W+ หมายถึงกลุ่มอักขระที่ไม่ใช่ตัวอักษรหรือตัวเลข)
           .filter(w => w.length > 4 && !stopWords.contains(w)) // 2. กรองเฉพาะคำที่มีความยาวมากกว่า 4 ตัวอักษร และต้องไม่อยู่ในกลุ่ม Stop Words
           .groupBy(identity) // 3. จัดกลุ่มคำที่เหมือนกันเข้าด้วยกัน โดยใช้ฟังก์ชัน identity (ใช้ตัวมันเองเป็น Key)
           // 4. นับจำนวนครั้งที่พบในแต่ละคำ
           .map { case (word, list) => (word, list.length) }
           // 5. แปลงเป็น List จัดเรียงลำดับตามจำนวนจากมากไปน้อย (Descending order) และเลือกมาเฉพาะ 5 อันดับแรก
           .toList.sortBy(-_._2).take(5)
  }

  // =========================================================
  // 2. การประมวลผลแบบขนานระดับข้อมูล (Data-Level Parallelism)
  // =========================================================
  
  def averageRatingByBranchPar(reviews: List[Review]): Map[String, Double] = {
    reviews.par.groupBy(_.branchId).map { case (branch, list) => // แปลง Collection เป็น Parallel Collection ด้วย .par ก่อนเริ่มการประมวลผล
      val avg = list.map(_.ratingPercent).sum[Double] / list.size
      (branch, avg)
    }.seq // แปลงผลลัพธ์จาก ParMap กลับเป็น Map ปกติ (Sequential Collection) ให้ตรงกับ Return Type
  }

  def countByContinentPar(reviews: List[Review]): Map[String, Int] = {
    reviews.par.groupBy(_.continent).map { case (cont, list) => 
      (cont, list.size) 
    }.seq 
  }

  def findTopWordsPar(reviews: List[Review]): List[(String, Int)] = {
    reviews.par.flatMap(r => r.reviewText.toLowerCase.split("\\W+"))
           .filter(w => w.length > 4 && !stopWords.contains(w))
           .groupBy(identity)
           .map { case (word, list) => (word, list.size) } 
           // จำเป็นต้องใช้ .seq เพื่อแปลงข้อมูลกลับเป็น Sequential Collection 
           // ก่อนที่จะทำการแปลงเป็น List และจัดเรียงลำดับ (Sort) เพื่อความถูกต้องของข้อมูลใน Thread หลัก
           .seq
           .toList.sortBy(-_._2).take(5)
  }
