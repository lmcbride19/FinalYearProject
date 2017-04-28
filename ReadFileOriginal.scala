import scala.io.Source
import util.control.Breaks._
import java.io.{ FileNotFoundException, IOException}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import java.lang.System.nanoTime


object ReadFileScala {
def main(args: Array[String]) {
/////////////////////////////////////////////////////////////////////////File and IsEmpty Defintion/////////////////////////////////////////////////////////////////////////
val filename = "/home/luke/Desktop/TestFiles/Test1.0.1.txt"

//println("Please Type in FileName to begin: ")
//val filename2 = scala.io.StdIn.readLine()
val start = System.nanoTime();
     println("Time at Start : " + start + "ns");
def isEmpty(x: String) = x == null || x.isEmpty

/////////////////////////////////////////////////////////////////////////Variable Declarations/////////////////////////////////////////////////////////////////////////
var a = 0; var b =0;
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
var CheckCounter = 0;
/////////////////////////////////////////////////////////////////////////ArrayBuffer and ListBuffer for Processing/////////////////////////////////////////////////////////////////////////
var Matched_Green = new ListBuffer[String]
var Matched_Amber = new ListBuffer[String]
var Matched_Red = new ListBuffer[String]
var DifLines = new ArrayBuffer[String]
DifLines +=" ";
DifLines +=" ";
DifLines +=" ";
DifLines +=" ";
/////////////////////////////////////////////////////////////////////////Try/Catch Statement and Processing/////////////////////////////////////////////////////////////////////////
try {
for (line <- Source.fromFile(filename).getLines)
{

a =a + 1;
if (isEmpty(line)) {
// Blank filename Check Statement
 /** println(" Blank filename Check ")
print("Blank filename Found at Number: ")
println(a) */
CheckCounter =0;
/////////////////////////////////////////////////////////////////////////Start of Pattern Matching Using Reguilar Expressions/////////////////////////////////////////////////////////////////////////
val match1 = Pattern_Security.findFirstIn(DifLines(1))
match1 match {
    case Some(s) => Security_Count = Security_Count + 1; CheckCounter = CheckCounter +1;//println(Security_Count)
    case None => 
}
val match2 = Pattern_Bomb.findFirstIn(DifLines(1))
match2 match {
    case Some(s) => Bomb_Count = Bomb_Count + 1; CheckCounter = CheckCounter +1;//println(Bomb_Count)
    case None => 
}
val match3 = Pattern_Terrorist.findFirstIn(DifLines(1))
match3 match {
    case Some(s) => Terrorist_Count = Terrorist_Count + 1;CheckCounter = CheckCounter +1; //println(Terrorist_Count)
    case None => 
}
val match4 = Pattern_Alert.findFirstIn(DifLines(1))
match4 match {
    case Some(s) => Alert_Count = Alert_Count + 1; CheckCounter = CheckCounter +1;//println(Alert_Count)
    case None => 
}
val match5 = Pattern_Danger.findFirstIn(DifLines(1))
match5 match {
    case Some(s) => Danger_Count = Danger_Count + 1; CheckCounter = CheckCounter +1;//println(Danger_Count)
    case None => 

}
val match6 = Pattern_President.findFirstIn(DifLines(1))
match6 match {
    case Some(s) =>President_Count = President_Count + 1; CheckCounter = CheckCounter +1;//println(President_Count)
    case None => 
}
val match7 = Pattern_Safety.findFirstIn(DifLines(1))
match7 match {
    case Some(s) =>Safety_Count = Safety_Count + 1; CheckCounter = CheckCounter +1;//println(Safety_Count)
    case None =>
}
val match8 = Pattern_liability.findFirstIn(DifLines(1))
match8 match {
    case Some(s) => Liability_Count = Liability_Count + 1; CheckCounter = CheckCounter +1;//println(Liability_Count)
    case None => 
}
val match9 = Pattern_SafeGuards.findFirstIn(DifLines(1))
match9 match {
    case Some(s) => SafeGuards_Count = SafeGuards_Count + 1; CheckCounter = CheckCounter +1;//println(SafeGuards_Count)
    case None =>
}
val match10 = Pattern_Comfort.findFirstIn(DifLines(1))
match10 match {
    case Some(s) => Comfort_Count = Comfort_Count + 1; CheckCounter = CheckCounter +1;//println(Comfort_Count)
    case None =>
}
val match11 = Pattern_Confidence.findFirstIn(DifLines(1))
match11 match {
    case Some(s) => Confidence_Count = Confidence_Count + 1; CheckCounter = CheckCounter +1;//println(Confidence_Count)
    case None => 
}
val match12 = Pattern_Collateral.findFirstIn(DifLines(1))
match12 match {
    case Some(s) => Collateral_Count = Collateral_Count + 1;CheckCounter = CheckCounter +1; //println(Collateral_Count)
    case None => 
}
val match13 = Pattern_Protection.findFirstIn(DifLines(1))
match13 match {
    case Some(s) => Protection_Count = Protection_Count + 1; CheckCounter = CheckCounter +1;//println(Protection_Count)
    case None => 
}
val match14 = Pattern_Vunerability.findFirstIn(DifLines(1))
match14 match {
    case Some(s) => Vunerability_Count = Vunerability_Count + 1;  CheckCounter = CheckCounter +1;//println(Vunerability_Count)
    case None => 
}
val match15 = Pattern_Defence.findFirstIn(DifLines(1))
match15 match {
    case Some(s) => Defence_Count = Defence_Count + 1; CheckCounter = CheckCounter +1; //println(Defence_Count)
    case None => 
}
val match16 = Pattern_Protocol.findFirstIn(DifLines(1))
match16 match {
    case Some(s) => Protocol_Count = Protocol_Count + 1; CheckCounter = CheckCounter +1;//println(Protocol_Count)
    case None => 
}
val match17 = Pattern_Solidness.findFirstIn(DifLines(1))
match17 match {
    case Some(s) => Solidness_Count = Solidness_Count + 1; CheckCounter = CheckCounter +1;//println(Solidness_Count)
    case None => 
}
val match18 = Pattern_Dependability.findFirstIn(DifLines(1))
match18 match {
    case Some(s) => Dependability_Count = Dependability_Count + 1; CheckCounter = CheckCounter +1;//println(Dependability_Count)
    case None => 
}
val match19 = Pattern_Toxic.findFirstIn(DifLines(1))
match19 match {
    case Some(s) => Toxic_Count = Toxic_Count + 1; CheckCounter = CheckCounter +1;//println(Toxic_Count)
    case None => 
}
val match20 = Pattern_Hazard.findFirstIn(DifLines(1))
match20 match {
    case Some(s) => Hazard_Count = Hazard_Count + 1;CheckCounter = CheckCounter +1; //println(Hazard_Count)
    case None => 
}
/////////////////////////////////////////////////////////////////////////Assign Risk/////////////////////////////////////////////////////////////////////////
if (CheckCounter <=5)
Matched_Green += DifLines(0) + DifLines(2)
else if (CheckCounter > 5 && CheckCounter <=15)
Matched_Amber += DifLines(0) + DifLines(2)
else (CheckCounter  > 15 )
Matched_Red += DifLines(0) + DifLines(2)

/////////////////////////////////////////////////////////////////////////Clear ArrayBuffer For Next Website/////////////////////////////////////////////////////////////////////////

DifLines.clear()
DifLines +=" ";
DifLines +=" ";
DifLines +=" ";
DifLines +=" ";
b = b+1;
}
/////////////////////////////////////////////////////////////////////////Assigning Each Letter to a specific Value In DifLines ArrayBuffer/////////////////////////////////////////////////////////////////////////
else if( line.charAt(0)== 'P'  ){ DifLines(0) = line                       }
else if( line.charAt(0)== 'Q'  ){  DifLines(1)= DifLines(1) + line  }
else if( line.charAt(0)== 'T'  ) {   DifLines(2) = line                     }
else if( line.charAt(0)== 'L'  ) { DifLines(3) = DifLines(3) + line }
/////////////////////////////////////////////////////////////////////////Error Statament If line Starts with Incorrect Character/////////////////////////////////////////////////////////////////////////
else if( line.charAt(0)!= 'L'  &&  line.charAt(0)!= 'T'  && line.charAt(0)!= 'Q'  && line.charAt(0)!= 'P') {
println("Exception - Error 1 (Incorrect Char value found, Must be PQTL)")
print("Character Found: ") 
println(line)
print("line Number: ")
println(a)
//break;
}
}

/////////////////////////////////////////////////////////////////////////Print Statements for Security Level Threats/////////////////////////////////////////////////////////////////////////
 if(Matched_Green.isEmpty){  println("Green Security Level Has No Entries") }
else { println(" Green Security Websites Check")
	     Matched_Green.foreach(println(_)) }
if(Matched_Amber.isEmpty){  println("Amber Security Level Has No Entries") }
else {   println(" Amber Security Websites Check")
	  Matched_Amber.foreach(println(_)) }
if(Matched_Red.isEmpty){  println("Red Security Level Has No Entries") }
else {   println(" Red Security Websites Check- These may need Checked Immediately")
	  Matched_Red.foreach(println(_)) } 
}
/////////////////////////////////////////////////////////////////////////Error Statement if Incorrect File name Attached/////////////////////////////////////////////////////////////////////////
 catch {
  case ex: FileNotFoundException => println("Exception- Error 2 (The File does not exist-Please Try Again)")
  case ex: IOException => println("Exception- Error 3 (Had an IOException trying to read that file)")
}
val end = System.nanoTime();
println("Time at End : " + end + "ns");
  
println("Time of program : " + (end - start) + "ns");

}

}
















