import spark.implicits._

/* Upload the dataset file */
val dataset_holiday=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment2/Dataset_Holidays.txt")

/*************************************************/
/* Problem Statement 1 - Start                   */
/* Route Generating the most revenue per year    */

println("###############################################################################")
println("      Problem 1 - Route Generating the most revenue per year                                ")
println("###############################################################################")
val groupBySrcDestYear=dataset_holiday.filter(dataset_holiday("_c3")==="airplane").groupBy("_c1","_c2","_c5").agg(sum("_c4")).sort("_c5").toDF("src","dest","year","totalrevenue")

val groupByYear=groupBySrcDestYear.groupBy("year").agg(max("totalrevenue"))

groupByYear.join(groupBySrcDestYear,"year").where($"max(totalrevenue)"===$"totalrevenue").select("year","src","dest","totalrevenue").show

/* Problem Statement 1 - End                     */
/*************************************************/

/*************************************************/
/* Problem Statement 2 - Start                   */

println("###############################################################################")
println("      Problem 2 - Total amount spent by every user in air travel per year      ")
println("###############################################################################")

val groupByTraverllerYear=dataset_holiday.filter(dataset_holiday("_c3")==="airplane").groupBy("_c0","_c5").agg(sum("_c4")).sort("_c0","_c5").toDF("traveller","year","amount")

groupByTraverllerYear.show(30)



/* Problem Statement 2 - End                     */
/*************************************************/

/*************************************************/
/* Problem Statement 3 - Start                   */
/* Which user has travelled largest distance till date */

println("###############################################################################")
println("      Problem 3 - Which age group is travelling the most                       ")
println("###############################################################################")

/* Get the sum of distance per year of per traveller */
val groupByTotDistanceByPerson=dataset_holiday.groupBy("_c5","_c0").agg(sum("_c4")).sort("_c5","_c0").toDF("year","traveller","distance")

/* Read the user details input file to get the age of each user */
val dataset_users=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment2/Dataset_User_details.txt").toDF("traveller","name","age")

/* Now, join the above two dataframes to include age in the output */
val groupTotDistanceByAge=groupByTotDistanceByPerson.join(dataset_users,"traveller").select("traveller","age","year","distance")

/* Now, add another column to display the age range */
val groupDistanceByAgeRange=groupTotDistanceByAge.withColumn("agerange",when($"age" < 20,"<20").otherwise(when($"age">=20 and $"age"<=35,"20-35").otherwise(when ($"age">35,">35") )))

/* Get total distance per age range */
val sumDistanceByAgeRange=groupDistanceByAgeRange.groupBy("agerange","year").agg(sum("distance")).toDF("agerange","year","totaldistance")

/* Get the max distance for each age range from the above dataframe */
val maxDistanceByYear=sumDistanceByAgeRange.groupBy("year").agg(max("totaldistance")).toDF("year","maxtotdistance")

/* Display the final output */
sumDistanceByAgeRange.join(maxDistanceByYear,"year").where($"totaldistance"===$"maxtotdistance").select("year","agerange","maxtotdistance").sort("year").show





/* Problem Statement 3 - End                     */
/*************************************************/


