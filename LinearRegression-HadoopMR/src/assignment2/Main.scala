import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

public class KalmanMain {
	public static void main(String args[]) throws IOException{
		//initialize files and reader/writers
		File source = new File("Assignment2-AccData.txt");
		File filter = new File("filter.txt");
		FileWriter filter_wr = new FileWriter(filter, true);
		BufferedReader source_rd = new BufferedReader(new FileReader(source));
		//empty filter file
		filter_wr.write("");
		String content = "";
		String result = "";
		KalmanFilter kf = new KalmanFilter();
		int cnt = 0, peak_cnt = 0;
		double [] data = new double[6];
		double[] judgeBuffer = {0,0,0};
		double [] FFT = new double[7935];
		double [] FFTres = new double[7935];
		int previoustime=0;
		source_rd.readLine();//skip the first line of titles
		while((content = source_rd.readLine()) != null){
			//System.out.println(cnt + "  "+ content);
			cnt++;
			//result = "";
			//contentSplits[0]=timestamp [1]=x [2]=y [3]=z length=4
			//data[0-4]=contentsplits data[5]=magnitude data[6]=kalman estimate
			String[] contentSplits = content.split("\t");
			for(int i=0;i<contentSplits.length;i++)
				data[i] = Double.parseDouble(contentSplits[i]);
			data[4] = Math.sqrt(data[1]*data[1]+data[2]*data[2]+data[3]*data[3]);
			FFT[cnt-1]=data[4];
			//data[5] = kf.update(data[4]);
			/*for(int i=0;i<data.length;i++)
				result+=(data[i]+"\t");
			result +="\n";
			filter_wr.write(result);
			if(data[0]-previoustime>300){
				if(judge(judgeBuffer=shiftAndAppend(judgeBuffer,data[5]))){			
					peak_cnt++;
					previoustime=(int) data[0];
				}
			}
			else{
				judgeBuffer[0]=0;
				judgeBuffer[1]=0;
				judgeBuffer[2]=0;
			}
			*/
		}
		 FastFourierTransformer transformer = new FastFourierTransformer(null);
		    try {           
		        Complex[] complx = transformer.transform(FFT, TransformType.FORWARD);

		        for (int i = 0; i < complx.length; i++) {               
		            double rr = (complx[i].getReal());
		            double ri = (complx[i].getImaginary());

		            FFTres[i] = Math.sqrt((rr * rr) + (ri * ri));
		        }

		    } catch (IllegalArgumentException e) {
		        System.out.println(e);
		    }
		 for(int i=0;i<FFTres.length;i++){
				result = FFTres[i]+"\n";
				filter_wr.write(result);
		 }
	//	System.out.println(peak_cnt);
	}
	public static double[] shiftAndAppend(double [] buffer,double value){
		buffer[0]=buffer[1];
		buffer[1]=buffer[2];
		buffer[2]=value;
		return buffer;
	}
	public static boolean judge(double [] buffer){
		if(buffer[0]*buffer[1]*buffer[2]==0)
			return false;
		if(buffer[1]>buffer[0]&&buffer[1]>buffer[2]){
			return true;//local maximum
		}
		else{
			return false;//not local maximum
		}
	}
}
