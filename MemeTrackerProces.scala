import scala.io.Source
import util.control.Breaks._
import java.io.{ FileNotFoundException, IOException}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
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

object MemeTrack {
def main(args: Array[String]) {

/*//val conf = new SparkConf().setAppName("PreProcessInput").setMaster("local")
//val sc = new SparkContext(conf)
the above will be used within spark-submit but is commented out of this for testing purposes
*/

val conf = new Configuration(sc.hadoopConfiguration) 
def paragraphFile(sc:org.apache.spark.SparkContext, path:String) : org.apache.spark.rdd.RDD[String] = {
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("textinputformat.record.delimiter", "\n\n")
    return sc.newAPIHadoopFile(path, classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
        classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text], conf).map(_._2.toString)
}

val input = paragraphFile(sc, "/home/luke/Desktop/TestFiles/Test1.0.1.txt")
var lines = input.repartition(sc.defaultParallelism)
println("Rdd initialised from TextFile...")

///////////////////////////////////////////////////////////////////////// Variable Declarations /////////////////////////////////////////////////////////////////////////
val Pattern_Security = new Regex("security")
var Security_Count =0;
val Pattern_Bomb = new Regex("bomb")
var Bomb_Count = 0;
val Pattern_Terrorist = new Regex("terrorist")
var Terrorist_Count = 0;
val Pattern_Alert = new Regex("alert")
var Alert_Count = 0;
val Pattern_Danger = new Regex("danger")
var Danger_Count =0;
val Pattern_President = new Regex("president")
var President_Count =0;
val Pattern_Safety = new Regex("safety")
var Safety_Count =0;
val Pattern_liability = new Regex("liability")
var Liability_Count =0;
val Pattern_SafeGuards = new Regex("safeguards")
var SafeGuards_Count =0;
val Pattern_Comfort= new Regex("comfort")
var Comfort_Count =0;
val Pattern_Confidence= new Regex("confidence")
var Confidence_Count =0;
val Pattern_Collateral = new Regex("collateral")
var Collateral_Count =0;
val Pattern_Protection = new Regex("protection")
var Protection_Count =0;
val Pattern_Vunerability = new Regex("vunerability")
var Vunerability_Count =0;
val Pattern_Defence= new Regex("defence")
var Defence_Count =0;
val Pattern_Protocol = new Regex("protocol")
var Protocol_Count =0;
val Pattern_Solidness= new Regex("solidness")
var Solidness_Count =0;
val Pattern_Dependability = new Regex("dependability")
var Dependability_Count =0;
val Pattern_Toxic = new Regex("toxic")
var Toxic_Count =0;
val Pattern_Hazard = new Regex("hazard")
var Hazard_Count =0;
var x = 0;
var counter = 0;
/////////////////////////////////////////////////////////////////////////ArrayBuffer + ListBuffer Declarations /////////////////////////////////////////////////////////////////////////
var DifLines = new ArrayBuffer[String]
var Matched_Green = new ListBuffer[String]
var Matched_Amber = new ListBuffer[String]
var Matched_Red = new ListBuffer[String]
///////////////////////////////////////////////////////////////////////// Function Definitions /////////////////////////////////////////////////////////////////////////
def ConcatFunc(a: String, b:String) : String = {
//Method 1
//var Concat = a + "---" + b;
//Method 2
var X = "---"
var Concat = a.concat(X);
var Concat2 = Concat.concat(b)
return Concat2;
} 
def ProcessObject(a: Array[String], b: ArrayBuffer[String]) : ArrayBuffer[String] = {
for(x <- a){
if( x.charAt(0)== 'P'  )       { b(0) = x                }
else if( x.charAt(0)== 'T'  ){  b(1)=  x }
else if( x.charAt(0)== 'Q'  ) {   b(2) = b(2) + " " + x                 }
else if( x.charAt(0)== 'L'  ) { b(3) = b(3) +  " " + x }
}
var TimeAndDate = ConcatFunc(b(0),b(1));

//println(TimeAndDate);
return b;
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
def StartArrayBuff(a : ArrayBuffer[String]) : ArrayBuffer[String] = {
a +=" ";
a +=" ";
a +=" ";
a +=" ";
//println("ArrayBuffer Started")
return a;
}
def ConvertToArray(a : String) : Array[String] = {
var Processed = a.split("\n")
//println("ConvertedToArray")
return Processed;
}
def NextToProcess(a : RDD[String], b : String) : RDD[String] = {
/**val Part = a.partitions.size;
val Default = sc.defaultParallelism
if (Part != Default)
{
var Next = a.filter(line => line!=b).repartition(sc.defaultParallelism);
return Next;
}
else
{
*/
var Next = a.filter(line => line!=b);
return Next;
}
def Copy(a : RDD[String], b : RDD[String]) : RDD[String] = {
var RDDCopy = a.union(b);
 return RDDCopy;
}
def ClearArrayBuff(a : ArrayBuffer[String]) : ArrayBuffer[String] = {
a.clear;
return a;
}
def ClearListBuff(a : ListBuffer[String]) : ListBuffer[String] = {
a.clear;
return a;
}
def PrintArrayTest(a : Array[String] ) : Unit = {
a.foreach(println)
}
def PrintArrayBuffTest(a : ArrayBuffer[String]) : Unit = { 
a.foreach(println)
}
def PrintListBuffTest(a : ListBuffer[String]) : Unit = { 
if (a.isEmpty == true)
println(" This list is empty ")
else
a.foreach(println)
}
def PrintRDDTest(a : RDD[String]) : Unit = { 
a.foreach(println)
}
def Matched_Greenfunc(a: ArrayBuffer[String] , b: Int, c: ListBuffer[String] ) : ListBuffer[String] = {
if (b > 2 && b <= 5){
c += ConcatFunc(a(0),a(1)); 
if (c.length == 100){
c.clear;
}
//println(" Green Working")
return c;}
else
//println(" Green Working")
return c;
}
def Matched_Amberfunc(a: ArrayBuffer[String] , b: Int, c: ListBuffer[String] ) : ListBuffer[String] = {
if (b > 5 && b <=15){
c += ConcatFunc(a(0),a(1)); 
if (c.length == 100){
c.clear;
}
//println(" Amber Working")
return c;}
else
//println(" Amber Working")
return c;
}
def Matched_Redfunc(a: ArrayBuffer[String] , b: Int, c: ListBuffer[String] ) : ListBuffer[String] = {

if (b  > 15){
c += ConcatFunc(a(0),a(1)); 
if (c.length == 100){
c.clear;
} 
//println(" Red Working")
return c;}
else
//println(" Red Working")
return c;
}
def PatternMatch(DifLines: ArrayBuffer[String]) : Int ={
var CheckCounter = 0;
/////////////////////////////////////////////////////////////////////////Start of Pattern Matching Using Reguilar Expressions/////////////////////////////////////////////////////////////////////////
val match1 = Pattern_Security.findFirstIn(DifLines(2))
match1 match {
    case Some(s) => Security_Count = Security_Count + 1; CheckCounter = CheckCounter +1;//println(Security_Count)
    case None => 
}
val match2 = Pattern_Bomb.findFirstIn(DifLines(2))
match2 match {
    case Some(s) => Bomb_Count = Bomb_Count + 1; CheckCounter = CheckCounter +1;//println(Bomb_Count)
    case None => 
}
val match3 = Pattern_Terrorist.findFirstIn(DifLines(2))
match3 match {
    case Some(s) => Terrorist_Count = Terrorist_Count + 1;CheckCounter = CheckCounter +1; //println(Terrorist_Count)
    case None => 
}
val match4 = Pattern_Alert.findFirstIn(DifLines(2))
match4 match {
    case Some(s) => Alert_Count = Alert_Count + 1; CheckCounter = CheckCounter +1;//println(Alert_Count)
    case None => 
}
val match5 = Pattern_Danger.findFirstIn(DifLines(2))
match5 match {
    case Some(s) => Danger_Count = Danger_Count + 1; CheckCounter = CheckCounter +1;//println(Danger_Count)
    case None => 

}
val match6 = Pattern_President.findFirstIn(DifLines(2))
match6 match {
    case Some(s) =>President_Count = President_Count + 1; CheckCounter = CheckCounter +1;//println(President_Count)
    case None => 
}
val match7 = Pattern_Safety.findFirstIn(DifLines(2))
match7 match {
    case Some(s) =>Safety_Count = Safety_Count + 1; CheckCounter = CheckCounter +1;//println(Safety_Count)
    case None =>
}
val match8 = Pattern_liability.findFirstIn(DifLines(2))
match8 match {
    case Some(s) => Liability_Count = Liability_Count + 1; CheckCounter = CheckCounter +1;//println(Liability_Count)
    case None => 
}
val match9 = Pattern_SafeGuards.findFirstIn(DifLines(2))
match9 match {
    case Some(s) => SafeGuards_Count = SafeGuards_Count + 1; CheckCounter = CheckCounter +1;//println(SafeGuards_Count)
    case None =>
}
val match10 = Pattern_Comfort.findFirstIn(DifLines(2))
match10 match {
    case Some(s) => Comfort_Count = Comfort_Count + 1; CheckCounter = CheckCounter +1;//println(Comfort_Count)
    case None =>
}
val match11 = Pattern_Confidence.findFirstIn(DifLines(2))
match11 match {
    case Some(s) => Confidence_Count = Confidence_Count + 1; CheckCounter = CheckCounter +1;//println(Confidence_Count)
    case None => 
}
val match12 = Pattern_Collateral.findFirstIn(DifLines(2))
match12 match {
    case Some(s) => Collateral_Count = Collateral_Count + 1;CheckCounter = CheckCounter +1; //println(Collateral_Count)
    case None => 
}
val match13 = Pattern_Protection.findFirstIn(DifLines(2))
match13 match {
    case Some(s) => Protection_Count = Protection_Count + 1; CheckCounter = CheckCounter +1;//println(Protection_Count)
    case None => 
}
val match14 = Pattern_Vunerability.findFirstIn(DifLines(2))
match14 match {
    case Some(s) => Vunerability_Count = Vunerability_Count + 1;  CheckCounter = CheckCounter +1;//println(Vunerability_Count)
    case None => 
}
val match15 = Pattern_Defence.findFirstIn(DifLines(2))
match15 match {
    case Some(s) => Defence_Count = Defence_Count + 1; CheckCounter = CheckCounter +1; //println(Defence_Count)
    case None => 
}
val match16 = Pattern_Protocol.findFirstIn(DifLines(2))
match16 match {
    case Some(s) => Protocol_Count = Protocol_Count + 1; CheckCounter = CheckCounter +1;//println(Protocol_Count)
    case None => 
}
val match17 = Pattern_Solidness.findFirstIn(DifLines(2))
match17 match {
    case Some(s) => Solidness_Count = Solidness_Count + 1; CheckCounter = CheckCounter +1;//println(Solidness_Count)
    case None => 
}
val match18 = Pattern_Dependability.findFirstIn(DifLines(2))
match18 match {
    case Some(s) => Dependability_Count = Dependability_Count + 1; CheckCounter = CheckCounter +1;//println(Dependability_Count)
    case None => 
}
val match19 = Pattern_Toxic.findFirstIn(DifLines(2))
match19 match {
    case Some(s) => Toxic_Count = Toxic_Count + 1; CheckCounter = CheckCounter +1;//println(Toxic_Count)
    case None => 
}
val match20 = Pattern_Hazard.findFirstIn(DifLines(2))
match20 match {
    case Some(s) => Hazard_Count = Hazard_Count + 1;CheckCounter = CheckCounter +1; //println(Hazard_Count)
    case None => 
}
//println(CheckCounter)
return CheckCounter;
}
def Processing(a: RDD[String], b: ArrayBuffer[String], c:ListBuffer[String], d: ListBuffer[String], e :ListBuffer[String]) : Unit  = {

if(a.isEmpty() == true)
{
println(" Green Articles: " ); 
PrintListBuffTest(c)
println(" Amber Articles: " ); 
PrintListBuffTest(d)
println(" Red Articles: " ); PrintListBuffTest(e)
println("Processing Finished")
a.unpersist();
}

else {
                                           x = x +1; 
                                           println("RDD element No: " + x)

                                           var RDD1 = TakeFirstRDD(a);
                                           var Con = ConvertToArray(RDD1)
                                           StartArrayBuff(b)
                                           ProcessObject(Con,b)
                                           var Check = PatternMatch(b)
                                           Matched_Greenfunc(b,Check,c)
                                           Matched_Amberfunc(b,Check,d)
                                           Matched_Redfunc(b,Check,e)
                                           var NXT = NextToProcess(a,RDD1)
                                           NXT.count();
                                           a.unpersist();
                                           ClearArrayBuff(b)

if (counter == 0) {

                                  println(" RDD Reference staying Correct here...." )
                                  var CopyRDD: RDD[String] = sc.emptyRDD[String]
                                  var Check = Copy(CopyRDD,NXT);
                                  counter = 100;
                                  Check.count();
                                  CopyRDD.unpersist();
                                  NXT.unpersist();
                                  var Parti = Check.repartition(sc.defaultParallelism)
                                  //Parti.cache();
                                  Processing(Parti,b,c,d,e)
                           }

else {

                                 counter = counter - 1;
                                 var Parti = NXT.repartition(sc.defaultParallelism)
                                //Parti.cache();
                                 Parti.count();
                                 NXT.unpersist();
                                 Processing(Parti,b,c,d,e)
       }
}
}
///////////////////////////////////////////////////////////////////////// Functions Finished ///////////////////////////////////////////////////////////////////////// 
///////////////////////////////////////////////////////////////////////// Main Method and Function Calls ///////////////////////////////////////////////////////////////////////// 

println("Processing.... ")
val start = System.nanoTime();
     println("Time at Start : " + start + "ns");

Processing(lines,DifLines,Matched_Green, Matched_Amber, Matched_Red)

val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");

}
}
