import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class EarthQuakesPerDateReducer extends Reducer<Text, Text, Text, 
Text> {
 protected void reduce(Text key,  Iterable<Text> values, Context context)
  throws IOException, InterruptedException {
	
	 	Double magAvg[] = new Double[10];
	 	Integer count[] = new Integer[10];
		for (int i=0;i<10;i++)
		{
			magAvg[i] = 0.0d;
			count[i] = 0;
		}
		int section = 0;
		for(Text val:values)
		{
			//Read and assign average values for each section of day
			String[] strList = val.toString().split("\\s+");
			double magnitude = Double.parseDouble(strList[0]);
			section = Integer.parseInt(strList[1]);
			magAvg[section-1] = magAvg[section-1] + magnitude;
			count[section-1] = count[section-1]+1;
		}

		String keyOutput = key + " " + (section);
		double average = magAvg[section-1]/count[section-1];
		String valueOutput = "avg mag:"+(average)+" quake cnt:"+count[section-1];
		context.write(new Text(keyOutput), new Text(valueOutput));
 }
 
 	private double maxMagni(double magni){
	 	double maxValue = Double.MIN_VALUE;
	    return maxValue = Math.max(maxValue, magni);
	  }
 
 
 
}