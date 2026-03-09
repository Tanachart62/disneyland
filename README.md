โปรเจควิชา FUNDAMENTAL WEB PROGRAMMING
## ภาพรวมโปรเจกต์ (Executive Summary)
* **วัตถุประสงค์หลัก:** พัฒนาระบบเพื่อทำความสะอาดข้อมูลดิบจาก CSV นำมาวิเคราะห์ทางสถิติ และ Export ออกมาเป็นรูปแบบ JSON
* **จุดเด่นทางเทคนิค:** มีการนำกระบวนทัศน์การประมวลผล 3 รูปแบบมาเปรียบเทียบความเร็ว ได้แก่ Sequential, Task-Level Parallelism (Future) และ Data-Level Parallelism (Parallel Collections)
* **โครงสร้างการเขียนโค้ด:** ใช้ Functional Programming (FP) ผสมผสานกับการจัดการ Error อย่างปลอดภัยด้วย `Try` และ `Either`

---


## 🔄 โครงสร้างสถาปัตยกรรม

ระบบถูกแบ่งออกเป็นโมดูลย่อยเพื่อให้ง่ายต่อการบำรุงรักษา (Modular Design):

1. **Extract (`ReviewDataLoader.scala`)**
   * อ่านข้อมูลจากไฟล์ `DisneylandReviews.csv`
   * ป้องกันโปรแกรม Crash จากไฟล์ที่ไม่ได้มาตรฐานด้วย `Codec("UTF-8")` และ `CodingErrorAction.REPLACE`
   * ใช้ `Either[String, Review]` ในการแยกข้อมูลที่สมบูรณ์ (Good Data) และตัดข้อมูลที่ฟิลด์ไม่ครบหรือผิดฟอร์แมตออก (Bad Data)

2. **Transform (`DataTransformer.scala`)**
   * **Rating:** แปลงคะแนนดาว (1-5) เป็นระบบเปอร์เซ็นต์ (0-100%)
   * **Date:** แยกปีและเดือน จัดการค่า "missing" ให้กลายเป็น `None` (เพื่อแปลงเป็น `null` ใน JSON)
   * **Location:** แมปชื่อประเทศของนักท่องเที่ยวให้กลายเป็นทวีป (Continent)
   * **Branch ID:** ย่อชื่อสาขา (เช่น Disneyland_HongKong -> HKG)
   * *หมายเหตุ: ใช้ `Try` ครอบการแปลง Type เพื่อป้องกัน Runtime Exception*

3. **Analyze (`ReviewAnalytics.scala`)**
   * คำนวณคะแนนรีวิวเฉลี่ยแยกตามสาขา (Average Rating by Branch)
   * สรุปจำนวนรีวิวแยกตามทวีปของนักท่องเที่ยว (Review Count by Continent)
   * ดึงคำศัพท์ที่ถูกใช้บ่อยที่สุด 5 อันดับแรก (Top 5 Words) พร้อมระบบตัดคำเชื่อม (Stop Words) ด้วย Regular Expression `\W+`

4. **Load (`Main.scala`)**
   * Export ผลการวิเคราะห์และสถิติเวลาการทำงานลงไฟล์ `analysis_output.json`
   * Export ข้อมูลที่ผ่านการคลีนแล้วทั้งหมดลงไฟล์ `transformed_data.json` เพื่อให้พร้อมนำไปใช้งานต่อ (Data Warehouse / API)

---

## ⏱️ การทดสอบประสิทธิภาพ (Performance Comparison)

โปรเจกต์นี้มีการประเมินและจับเวลา Execution Time ใน 3 รูปแบบ:

1. **Sequential Processing:** ประมวลผลข้อมูลตามลำดับจากบนลงล่างบน Thread หลัก
2. **Task-Level Parallelism (Future):** ใช้ `scala.concurrent.Future` โยนงานวิเคราะห์ 3 ฟังก์ชันให้ทำงานพร้อมกันเบื้องหลัง และรอผลลัพธ์ด้วย `Await.result`
3. **Data-Level Parallelism (.par):** กระจายก้อนข้อมูล (Dataset) ย่อยๆ ให้ CPU หลาย Core ช่วยกันประมวลผลพร้อมกันผ่าน Parallel Collections

*ระบบจะเปรียบเทียบและสรุปว่า Execution Mode ใดทำงานได้รวดเร็วที่สุดโดยอัตโนมัติ*

---