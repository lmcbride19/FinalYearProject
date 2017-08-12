/* Luke McBride 
BisectingKMeans Algorithm -Test File 
Line 51 needs changed depending on Location of Test File
Ctrl f Find => "/home/lmcbride19/Project_Summer/Higgs.csv"
         Replace => "Location of File"
Test File - Higgs.csv ~8gb
Link to Dataset -http://archive.ics.uci.edu/ml/datasets/HIGGS
*/
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.Row
import java.lang.System.nanoTime
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io._;
import java.io.Serializable;
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.clustering.BisectingKMeans

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 1 - Declare Object and a Main Class
Set up config(Note commented out as TestFiles are for Spark-Shell)
*/
object KMeansClusterBisect extends Serializable {
def main(args: Array[String]){
printf("Starting KMeansCluster......")
val conf = new SparkConf().setAppName("BisectingKMeans")
val sc = new SparkContext(conf)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 2 - Set up Start Time and Create a SQLContext based on our Spark Context(
sc Denotes the Spark-Shell pre setup Context)
*/
val start = System.nanoTime();
     println("Time at Start : " + start + "ns");
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 3 - Read in .CSV file of the data via the SQLContext using Databricks defintion of
CSV files, In this case the Csv file has no header so we set this to false,
InferSchema is set to True to save any conversions Later and the load option is the FilePath
*/
val HiggsData  = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .load("/home/lmcbride19/Project_Summer/Higgs.csv")
HiggsData.persist()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 4 - Define the Columns that will be used as the Features to Determine The Clusters
All columns Except the Original Label Column are used here 
(Data can also be used for Classification)
*/
val featureCols = Array("_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c8","_c9","_c10",
"_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c18","_c19",
"_c20","_c21","_c22","_c23","_c24","_c25","_c26","_c27","_c28")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 5 - For Machine Learning Algorithms within Spark the features need to be In Vector UDT format
The imported Method VectorAssembler Allows this to happen and Sets the Features to an
output column within the Dataset via the Transform() Method
The StringIndexer does the same for the label Column as sets it to label. These are now 
in the correct format for the Algorithm to process
*/
val VectorMethod = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val HiggsDataFeature= VectorMethod.transform(HiggsData)
val StringLabelMethod = new StringIndexer().setInputCol("_c0").setOutputCol("label")
val HiggsDataFinal = StringLabelMethod.fit(HiggsDataFeature).transform(HiggsDataFeature)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* Step 6 - Print Statement and Usual Split for the Data, not essential in Clustering so 
Commented out here
*/
printf("Setting up model.......\n")
//val splitSeed = 5043
//val Array(trainingData, testData) = TrainFinal.randomSplit(Array(0.7, 0.3), splitSeed)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 7 - Set up The BisectingKMeans Algorithm, setK is the number of clusters
in this case 2 as we want to compare with Classification with this specific Dataset
SetSeed is just the random split, 1 is fine here
We then use this Algorithm to fit to the Data
*/
val BisectingKMeansAlgo = new BisectingKMeans().setK(2).setSeed(1L)
val BisectingKMeansModel = BisectingKMeansAlgo.fit(HiggsDataFinal)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 8 - Print out the Cluster Centres for each of the features observed in the Features 
Column
*/
println("Cluster Centers For Bisecting KMeans: ")
val clusterCentres = BisectingKMeansModel.clusterCenters
clusterCentres.foreach(println)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 9 -Compute the cost of the model and what Based on the Set Sum of Squared Errors
*/
val cost = BisectingKMeansModel.computeCost(HiggsDataFinal)
println(s"Set Sum of Squared Errors = $cost")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 10 - End the Clock that is timing and Give an Overall Time in execution in NanoSeconds
Should be insignificant with these TestFiles
*/
val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");
}
}

