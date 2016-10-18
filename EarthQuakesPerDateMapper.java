import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class EarthQuakesPerDateMapper extends Mapper<LongWritable, 
  Text, Text, Text> {
 @Override
 protected void map(LongWritable key, Text value, Context context) throws IOException,
   InterruptedException {
  Boolean invalidData = false;
  if (key.get() > 0) {
   try {
	   String line=value.toString();
	   String arr[]=line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

	 String dtstr = getDate(arr[0],"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
     System.out.println(""+dtstr);
     double magnitude =0.0;
     if(arr[4] == null ||arr[4].isEmpty())
    	 invalidData = true;
     
	     if (!invalidData)
	     {
	    	 magnitude = Double.parseDouble(arr[4]);
	    	 if(magnitude>0.0){
	    		 Data d = new Data();
	    		 getMagGroup(d, magnitude);
			     String maggroup = d.maggroup;
			     int group = d.group;
			     String keyOutput = dtstr + " " +maggroup;
			     String valOutput = String.valueOf(magnitude)+" "+group;
			     context.write(new Text(keyOutput), new Text(valOutput));
	    	 }
	     }
   } catch (ParseException e) {}
  }
 }

 	 
	 
 
 private static void getMagGroup(Data data,double magnitude){
     if(magnitude>0 && magnitude<=1){
    	 data.maggroup ="(0,1]";
    	 data.group = 1;
     }else if(magnitude>1 && magnitude<=2){
    	 data.maggroup ="(1,2]";
    	 data.group = 2;
     }else if(magnitude>2 && magnitude<=3){
    	 data.maggroup ="(2,3]";
    	 data.group =3;
     }else if(magnitude>3 && magnitude<=4){
    	 data.maggroup ="(3,4]";
    	 data.group = 4;
     }else if(magnitude>4 && magnitude<=5){
    	 data.maggroup ="(4,5]";
    	 data.group = 5;
     }else if(magnitude>5 && magnitude<=6){
    	 data.maggroup ="(5,6]";
    	 data.group = 6;
     }else if(magnitude>6 && magnitude<=7){
    	 data.maggroup ="(6,7]";
    	 data.group = 7;
     }else if(magnitude>7 && magnitude<=8){
    	 data. maggroup ="(7,8]";
    	 data.group = 8;
     }else if(magnitude>8 && magnitude<=9){
    	 data.maggroup ="(8,9]";
    	 data.group = 9;
     }else{
    	 data.maggroup ="(9,>]";
    	 data.group = 10;
     }
	 
 }
 
 private String getWeek(String date,String format) throws ParseException{
	 SimpleDateFormat formatter = 
		       new SimpleDateFormat(format);
	   Date dt = formatter.parse(date);
	   formatter.applyPattern("dd-MM-yyyy");
		
	   Calendar calendar = new GregorianCalendar();
	   calendar.setTime(dt);  
	   String key = "Week "+ calendar.get(Calendar.WEEK_OF_YEAR) + " " + calendar.get(Calendar.YEAR);
	 
	 
	return key;
 }

 private String getDate(String date,String format) throws ParseException{
     SimpleDateFormat formatter = 
    	       new SimpleDateFormat(format);
    	     Date dt = formatter.parse(date);
    	     formatter.applyPattern("dd-MM-yyyy");
    	     String dtstr = formatter.format(dt);
    	     
    	     
			return dtstr;
 }

 private static void getLatGroup(Data data,double latitude){
	 
 }

}

class Data {
    public String maggroup;
    public int group;
}