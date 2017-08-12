/* Luke McBride 
OneVsAll Algorithm -Test File 
Line 48 needs changed depending on Location of Test File
Ctrl f Find => "/home/lmcbride19/Project_Summer/Hepmass_full.csv"
         Replace => "Location of File"
Test File - HepMass_full.csv ~18gb
Link to Dataset -https://archive.ics.uci.edu/ml/datasets/HEPMASS
*/

import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 1 - Declare Object and a Main Class
Set up config(Note commented out as TestFiles are for Spark-Shell)
*/
object OnevsAll extends Serializable {
def main(args: Array[String]){
printf("Starting OneVsAll......")

val conf = new SparkConf().setAppName("OneVsAll")
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
CSV files, In this case the Csv file has a header so we set this to true,Top line gets ommited from any
calculations on the dataset
InferSchema is set to True to save any conversions Later and the load option is the FilePath
*/
val HEPMASSData  = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/lmcbride19/Project_Summer/Hepmass_full.csv")
HEPMASSData.persist()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 4 - Define the Columns that will be used as the Features to Determine The Classification
All columns Except the Original Label Column are used here 
(Data can also be used for Clustering)
*/
val featureCols = Array("f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","f10",
"f11","f12","f13","f14","f15","f16","f17","f18","f19",
"f20","f21","f22","f23","f24","f25","f26")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 5 - For Machine Learning Algorithms within Spark the features need to be In Vector UDT format
The imported Method VectorAssembler Allows this to happen and Sets the Features to an
output column within the Datset
The StringIndexer does the same for the label Column as sets it to label. These are now 
in the correct format for the Algorithm to process as transform() has been called
*/
val VectorMethod = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val HEPMASSDataFeature = VectorMethod.transform(HEPMASSData)
val StringLabelMethod = new StringIndexer().setInputCol("# label").setOutputCol("label")
val HEPMASSDataFinal = StringLabelMethod.fit(HEPMASSDataFeature).transform(HEPMASSDataFeature)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* Step 6 - Print Statement and Usual Split for the Data into Training and Testing, 
*/
printf("Setting up model.......\n")
val splitSeed = 5043
val Array(trainingData, testData) = HEPMASSDataFinal.randomSplit(Array(0.7, 0.3), splitSeed)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 7 - Set up a Logistic Regression Algorithm, using maxiter at 10 again, setTol to 1E-6 and FitIntercept to True
We then use this Algorithm to fit into the OneVsRest algorithm this should give us similar results
to normal Logistic Regression as this is a Binary Classification issue but worth the test nonetheless
*/
val classifier = new LogisticRegression()
  .setMaxIter(10)
  .setTol(1E-6)
  .setFitIntercept(true)

val OneVsAll = new OneVsRest().setClassifier(classifier)
val OneVsAllModel = OneVsAll.fit(trainingData)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 8 -  we use the model on the test data
 to evaulate via the MulticlassClassificationEvaluator and see the accuracy of the model
 More Metrics can be printed but aren't so here 
*/
val OneVsAllpredictions = OneVsAllModel.transform(testData)
val OneVsAllevaluator = new MulticlassClassificationEvaluator()
  .setMetricName("accuracy")

val accuracy = OneVsAllevaluator.evaluate(OneVsAllpredictions)
println(s"Test Accuracy = ${accuracy}")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 9 - End the Clock that is timing and Give an Overall Time in execution in NanoSeconds
Should be insignificant with these TestFiles
*/
val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");

}
}
