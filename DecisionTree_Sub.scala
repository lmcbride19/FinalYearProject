/* Luke McBride 
DecisionTree Algorithm -Test File 
Line 52 needs changed depending on Location of Test File
Ctrl f Find => "/home/lmcbride19/Project_Summer/Hepmass_full.csv"
         Replace => "Location of File"
Test File - HepMass_full.csv ~18gb
Link to Dataset -https://archive.ics.uci.edu/ml/datasets/HEPMASS
*/
import java.lang.System.nanoTime
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.io._;
import java.io.Serializable;
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 1 - Declare Object and a Main Class
Set up config(Note commented out as TestFiles are for Spark-Shell)
*/
object DecisionTree extends Serializable {
def main(args: Array[String]){
printf("Starting DecisionTree......")
val conf = new SparkConf().setAppName("DecisionTree")
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
output column within the Datset via the Piepline later on
The StringIndexer does the same for the label Column as sets it to label. These are now 
in the correct format for the Algorithm to process when the pipeline is called
*/
val VectorMethod = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val StringLabelMethod = new StringIndexer().setInputCol("# label").setOutputCol("label")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/* Step 6 - Print Statement and Usual Split for the Data into Training and Testing
*/
printf("Setting up model.......\n")
val seed = 1234L
val Array(trainingData, testData) = HEPMASSData.randomSplit(Array(0.7, 0.3), seed)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 7 - Set up The Decisiontree Algorithm, setLabelColumn is the column Specified in 
Step 5 and Features is the Same, we call these Directly from the VectorAssembler and StringIndexer Calls
We set the number of maxBins to 1000 and MaxDepth to 15, MaxBins will change depending on how
much data there is available, the impurity is set to Entropy
We then use this Algorithm to fit to the Data
*/
val DecisionTreeAlgo = new DecisionTreeClassifier()
  .setLabelCol(StringLabelMethod.getOutputCol)
  .setFeaturesCol(VectorMethod.getOutputCol)
 .setImpurity("entropy")
  .setMaxBins(1000)
  .setMaxDepth(15)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 8 - We need to Create a Prediction Column so we can test our Model, this converts the 
Indexed labels from above to Normal again
*/
val PredictionLabelConverter = new StringIndexer()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 9 - Create the pipeline of processes to run and set the stages as an array of previosuly declared
values from step 5 - step 8
*/
val pipeline = new Pipeline()
  .setStages(Array(StringLabelMethod, VectorMethod ,DecisionTreeAlgo,PredictionLabelConverter))

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 10 - In this step we fit the pipeline to our training Data and create a DecisionTreeModel
This model is then used to Predict against the TestData
Showing us some information to help show the process
*/
val DecisionTreeModel = pipeline.fit(trainingData)
val PredictionsOnTestData = DecisionTreeModel.transform(testData)
PredictionsOnTestData.select("predictedLabel", "label", "features").show()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 11 - Here we use the MulticlassClassificationEvaluator to decipher some metrics from
the classifier, we input our prediction and the real label from here we can use the evaluate call
to check how well they match and give us an Accuracy Value
The Bottom Method shows us our trees in String format and how they where Derived
*/
val DecisionTreeEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = DecisionTreeEvaluator.evaluate(PredictionsOnTestData)
    println("Test Accuracy = " + (accuracy))

val DecisionTreeTreeModel = DecisionTreeModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println("Learned classification tree model:\n" + DecisionTreeTreeModel.toDebugString)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*Step 12 - End the Clock that is timing and Give an Overall Time in execution in NanoSeconds
Should be insignificant with these TestFiles
*/

val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");
}
}
