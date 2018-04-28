package edu.ucr.cs.cs226.khegd001.HDFSUpload;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class HDFSUpload {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		//String HDFSpath=args[1];
		long startTime = 0;
		
		
		try {
			String filename= args[0];
		Configuration conf = new Configuration();
		
			FileInputStream localFile= new FileInputStream(filename);
			InputStream inFile = new BufferedInputStream(localFile);
		
		Path path = new Path(args[1]);
		FileSystem fs = path.getFileSystem(conf);
		
		if (fs.exists(path)) {
            System.out.println("File Already exists in HDFS");
            
        }
		else {
			startTime = System.nanoTime();
			System.out.println("uploading file");
		FSDataOutputStream outFile = fs.create(path,new Progressable(){
          public void progress(){
             System.out.println("..");
		}
		});
		
		//  IOUtils.copyBytes(inFile, outFile, 8192, true);
		
		byte[] buffer = new byte[4096];
		int read = 0;
		while ((read = inFile.read(buffer, 0, buffer.length)) != -1) {
			outFile.write(buffer, 0, read);
		
		}
		outFile.close();
		
		long endTime = System.nanoTime();
			System.out.println("Time taken to upload file: "+(endTime - startTime)/1000000000.0 + " s"); 

		  
		}
		}
		catch(FileNotFoundException fne) {
			System.out.println("Cannot find the given file");
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("Incorrect arguments!!");
		}
		catch(Exception e) {
			System.out.println("File can not be created");
		}
		
		
	}
	
	//code
	
			

}
