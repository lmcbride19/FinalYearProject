import java.io.{ FileNotFoundException, IOException}
import scala.util.matching.Regex
import java.lang.System.nanoTime
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import java.io.Serializable;

object MemeTrack extends Serializable {
def main(args: Array[String]){
def paragraphFile(sc:org.apache.spark.SparkContext, path:String) : org.apache.spark.rdd.RDD[String] = {
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("textinputformat.record.delimiter", "\n\n")
    return sc.newAPIHadoopFile(path, classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
        classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text], conf).map(_._2.toString)
}
var BuzzWords = Array("security", "bomb","terrorist","alert","danger","president","safety","liability","safeguards","comfort","confidence"
,"collateral","protection","vunerability","defence","protocol","solidness","dependability","toxic","hazard")
var b ="QwertySpark2017"
def PatternMatchFilter( x : String) : String = {
var CheckCounter = 0;
for (y <- BuzzWords)
{
var regex = new Regex(y)
var matchReg =  regex.findFirstIn(x)
matchReg match { case Some(s) => CheckCounter = CheckCounter +1 ;   
case None => 
}
}
if (CheckCounter != 0)
{
return x;
}
else{
var Empty = "QwertySpark2017"
return Empty;
}

}

def PatternMatchGreen( x : String) : String = {
var CheckCounter = 0;

for (y <- BuzzWords)
{

var regex = new Regex(y)
var matchReg =  regex.findFirstIn(x)
matchReg match { case Some(s) => CheckCounter = CheckCounter +1 ;   
case None => 
}
}
if (CheckCounter <= 5)
{
return x;

}
else{
var Empty = "QwertySpark2017"
return Empty;
}

}

def PatternMatchAmber( x : String) : String = {
var CheckCounter = 0;

for (y <- BuzzWords)
{

var regex = new Regex(y)
var matchReg =  regex.findFirstIn(x)
matchReg match { case Some(s) => CheckCounter = CheckCounter +1 ;   
case None => 
}
}
if (CheckCounter > 5 && CheckCounter <=15)
{
return x;

}
else{
var Empty = "QwertySpark2017"
return Empty;
}

}

def PatternMatchRed( x : String) : String = {
var CheckCounter = 0;

for (y <- BuzzWords)
{

var regex = new Regex(y)
var matchReg =  regex.findFirstIn(x)
matchReg match { case Some(s) => CheckCounter = CheckCounter +1 ;   
case None => 
}
}
if (CheckCounter > 15)
{
return x;

}
else{
var Empty = "QwertySpark2017"
return Empty;
}
}

def PatternMatchFilterBackwards(x : String, BuzzWords : Array[String]) : String = {
var CheckCounter = 0;

for (y <- BuzzWords)
{

var regex = new Regex(y)
var matchReg =  regex.findFirstIn(x)
matchReg match { case Some(s) => CheckCounter = CheckCounter +1 ;   
case None => 
}
}
if (CheckCounter == 0)
{
return x;
}
else{
var Empty = "QwertySpark2017"
return Empty;
}

}

def TakeFirstRDD(a : RDD[String]) : String = { 
var X = a.first()
//println("First item of RDD took ")
return X;
}

def LineCount(a : RDD[String]) : Long = {
val Count = a.count();
return Count;
}

def PrintArrayTest(a : Array[String] ) : Unit = {
a.foreach(println)
}

def ConcatFunc(a: String, b:String) : String = {
//Method 1
//var Concat = a + "---" + b;
//Method 2
var X = "  ---  "
var Concat = a.concat(X);
var Concat2 = Concat.concat(b)
return Concat2;
} 
/*//val conf = new SparkConf().setAppName("PreProcessInput").setMaster("local")
//val sc = new SparkContext(conf)
the above will be used within spark-submit but is commented out of this for testing purposes
*/

try {
val input = paragraphFile(sc, "/home/luke/Desktop/Test1.1.txt")
var lines = input.repartition(sc.defaultParallelism)
val Partitions = sc.defaultParallelism
//Due to lazy transformations timing can be done here
println("Beginning Timer... ")
val start = System.nanoTime();
     println("Time at Start : " + start + "ns");
println("Rdd initialised from TextFile...")
println("Running on " + Partitions + " Partitions")

val Filtered = lines.map(x => PatternMatchFilter(x)).filter(x =>x!=b)
var Green =  Filtered.map(x => PatternMatchGreen(x)).filter(x => x!=b);
var Amber =  Filtered.map(x => PatternMatchAmber(x)).filter(x => x!=b);
var Red =  Filtered.map(x => PatternMatchRed(x)).filter(x => x!=b); 

var GreenCount = LineCount(Green)
var AmberCount = LineCount(Amber)
var RedCount = LineCount(Red)
println(" Green Security Websites Check: " + GreenCount)
println(" Amber Security Websites Check: " + AmberCount)
println(" Red Security Websites Check- These may need Checked Immediately: " + RedCount)

val GreenSplit = Green.map(x => x.split("\n")).map(x => ConcatFunc(x(0),x(1)) + "\n")
//GreenSplit.collect().foreach(println)
GreenSplit.coalesce(1).saveAsTextFile("file:///home/luke/Desktop/output/GreenOutput")
val AmberSplit = Amber.map(x => x.split("\n")).map(x => ConcatFunc(x(0),x(1)) + "\n")  
//AmberSplit.collect().foreach(println)
AmberSplit.coalesce(1).saveAsTextFile("file:///home/luke/Desktop/output/AmberOutput")
val RedSplit = Red.map(x => x.split("\n")).map(x => ConcatFunc(x(0),x(1)) + "\n")
//RedSplit.collect().foreach(println)
RedSplit.coalesce(1).saveAsTextFile("file:///home/luke/Desktop/output/RedOutput")

val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");
println("Processing Completed")
}
 catch {
  case ex: FileNotFoundException => println(ex)
  case ex: IOException => println(ex)
}
}
}