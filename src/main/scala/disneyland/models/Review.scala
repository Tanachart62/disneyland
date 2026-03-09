package disneyland.models
case class Review(
  id: Long, 
  ratingPercent: Double,  
  year: Option[Int],      
  month: Option[Int],     
  continent: String,      
  reviewText: String,
  branchId: String        
)