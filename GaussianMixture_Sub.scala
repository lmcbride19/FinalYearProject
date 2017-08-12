/* Luke McBride 
GaussianMixture Algorithm -Test File 
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
import sqlContext.implicits._
import org.apache.spark.ml.clustering.GaussianMixture
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 1 - Declare Object and a Main Class
Set up config(Note commented out as TestFiles are for Spark-Shell)
*/
object GaussianMixtureCluster extends Serializable {
def main(args: Array[String]){
printf("Starting GuassianMixture......")
val conf = new SparkConf().setAppName("GaussianMixtureModel")
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
/*Step 7 - Set up GaussianMixture Algorithm, setK is the number of clusters
in this case 2 as we want to compare with Classification with this specific Dataset
We then use this Algorithm to fit to the Data
*/
val GaussianMixtureAlgo = new GaussianMixture()
  .setK(2)
val GaussianMixtureModel = GaussianMixtureAlgo.fit(HiggsDataFinal)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 8 -Print out the Features of the Gaussian MixtureModel
*/
for (i <- 0 until GaussianMixtureModel.getK) {
  println("weight=%f\nmu=%s\nsigma=\n%s\n" format
    (GaussianMixtureModel.weights(i), GaussianMixtureModel.gaussians(i).mean,GaussianMixtureModel.gaussians(i).cov))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 9 - End the Clock that is timing and Give an Overall Time in execution in NanoSeconds
Should be insignificant with these TestFiles
*/
val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");
}
}