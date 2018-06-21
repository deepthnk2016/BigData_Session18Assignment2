import spark.implicits._

/* Upload the dataset file */
val dataset_holiday=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment1/Dataset_Holidays.txt")

/*************************************************/
/* Problem Statement 1 - Start                   */
/* Distribution of total air travellers per year */

println("###############################################################################")
println("      Problem 1 - Total air travellers per year                                ")
println("###############################################################################")
dataset_holiday.filter(dataset_holiday("_c3")==="airplane").groupBy("_c5").count().show()

/* Problem Statement 1 - End                     */
/*************************************************/

/*************************************************/
/* Problem Statement 2 - Start                   */

println("###############################################################################")
println("      Problem 2 - Total air distance covered by each user per year             ")
println("###############################################################################")

dataset_holiday.filter(dataset_holiday("_c3")==="airplane").groupBy("_c0","_c5").agg(sum("_c4")).show()

/* Problem Statement 2 - End                     */
/*************************************************/

/*************************************************/
/* Problem Statement 3 - Start                   */
/* Which user has travelled largest distance till date */

println("###############################################################################")
println("      Problem 3 - User which has travelled largest distance till date          ")
println("###############################################################################")

/* Find the sum of all the distances by each employee */
val df1=dataset_holiday.groupBy("_c0").agg(sum("_c4"))

/* Sort according to sum of distances and fetch the first row */
df1.sort(desc("sum(_c4)")).show(1)
/* Problem Statement 3 - End                     */
/*************************************************/

/*************************************************/
/* Problem Statement 4 - Start                   */
/* Most preferred destination for all the users   */

println("###############################################################################")
println("      Problem 4 - Most preferred destination for all the users                 ")
println("###############################################################################")

/* Count each destination */
val df1=dataset_holiday.groupBy("_c2").count()

/* Sort according to the count and display the first row */
df1.sort(desc("count")).show(1)
/* Problem Statement 4 - End                     */
/*************************************************/

