import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.io.IOException;
import java.io.EOFException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.concurrent.TimeUnit;
//Java code for thread creation by extending
//the Thread class



class sortMultiThread extends Thread{
private Thread t;
	String inputFile;
	String outputFile;
	private CountDownLatch latch;
	
	public sortMultiThread(String filePath, String tmpFilePath, CountDownLatch latch)  {
		inputFile = filePath;
		outputFile = tmpFilePath;
		this.latch = latch;
		//System.out.println(filePath + ", " + tmpFilePath);
		
	}

 public void run(){
     try {
			ArrayList<String> data = new ArrayList();
			String line = null;
			LineNumberReader reader;
			File inFile = new File(inputFile);
			long fileLength = (inFile.length())/100;
			reader = new LineNumberReader(new FileReader(inputFile));
			for(long i=0;i<fileLength;i++){
				line = reader.readLine();
				while(line == null && i < fileLength){
					line = reader.readLine();
				}
				data.add(line);
			}
			////System.out.println("Array size is " + data.size());
			reader.close();
			File f = new File(inputFile);
			f.delete();
			Collections.sort(data, new Comparator<String>() {
        	 public int compare(String str1, String str2){
        	 	if(str1 == null || str2 == null){
        	 		return 0;
        	 	}
        		 String substr1 = str1.substring(0,10);
        		 String substr2 = str2.substring(0,10);
        		 return substr1.compareTo(substr2);
        	 }
			});
			File tmpfile = new File(outputFile);
			FileWriter fileWriter = new FileWriter(tmpfile);
			for (String value : data) {
				fileWriter.write(value + "\r\n");
			}
			fileWriter.close();
			//System.out.println("output file length is " + tmpfile.length());
		} catch (IOException e) {
			e.printStackTrace();
		}
		 latch.countDown();
}



public void start () {
      if (t == null) {
         t = new Thread (this, "Thread");
         t.start ();
      }
   }

 }

class DataBuffer {
	public LineNumberReader reader;
	public File originalFile;
	private String bufferString;
	private boolean empty;
	private long fileLength;
	private long index = 0;
	public DataBuffer(File file) throws IOException {
		originalFile = file;
		fileLength = file.length()/100;
		reader = new LineNumberReader(new FileReader(file));
		getNextValue();
	}

	public boolean empty() {
		return empty;
	}

	private void getNextValue() throws IOException {
		try {
			while((bufferString = reader.readLine()) == null && index < fileLength){
				empty = false;
				index = index + 1;
			}
			if(bufferString == null){
				empty = true;
			}
			
		} catch (EOFException oef) {
			empty = true;
			bufferString = null;
		}
	}

	public void close() throws IOException {
		reader.close();
	}

	public String getBufferString() {
		return bufferString;
	}

	public String getFristValue() throws IOException {
		String value = getBufferString();
		getNextValue();
		return value;
	}

}


public class MySort {
	public static void performKWayMergeSort(List<File> sortedFiles, File outputFile, int numberofFiles) {

			PriorityQueue<DataBuffer> priorityQueue = new PriorityQueue<DataBuffer>((numberofFiles + 1),
				new Comparator<DataBuffer>() {
					public int compare(DataBuffer data1, DataBuffer data2) {
						String sub1 = data1.getBufferString();
						String sub2 = data2.getBufferString();
						if (sub1 == null || sub2 == null) {
							return 0;
						} else {
							sub1 = sub1.substring(0,10);
							sub2 = sub2.substring(0,10);
							return sub1.compareTo(sub2);
						}

					}
				});

		try {
			for (File eachFile : sortedFiles) {
				DataBuffer fileBuffer = new DataBuffer(eachFile);
				priorityQueue.add(fileBuffer);
			}

			FileWriter fileWriter = new FileWriter(outputFile);
			while (priorityQueue.size() > 0) {
				DataBuffer fileBuffer = priorityQueue.poll();
				String value = fileBuffer.getFristValue() + "\r\n";
				fileWriter.write(value);
				if (fileBuffer.empty()) {
					fileBuffer.close();
					fileBuffer.originalFile.delete();
				} else {
					priorityQueue.add(fileBuffer);
				}
			}
			fileWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws InterruptedException, IOException  {
		boolean append = true;
		FileHandler handler = new FileHandler("default.log", append);
		Logger logger = Logger.getLogger("MySort");
		String sortFileSize=args[0]; 
		int noOfThreads=Integer.parseInt(args[1]); //This represent the Number of thread to process each chunks
		long maxRecordCount=Long.parseLong(args[2]); //This represents the maximum records of each chunk. Size of chunk=maxRecordCount*100 (in bytes)
		String filePath=null;
		if("2GB".equalsIgnoreCase(sortFileSize)){
			filePath="data-2GB";
			handler = new FileHandler("mySort2GB.log",append); 
		}else if("20GB".equalsIgnoreCase(sortFileSize)){
			filePath="data-20GB";
			handler = new FileHandler("mySort20GB.log",append); 
			noOfThreads=1;
			maxRecordCount=2500000;
		}
		//System.out.println("No of Thread "+noOfThreads);
		logger.info("No of Thread "+noOfThreads);
		 SimpleFormatter formatter = new SimpleFormatter();  
        handler.setFormatter(formatter); 
        logger.addHandler(handler);
		//System.out.println("Sorting of file size maximum of records File :"+maxRecordCount);
		long size = 100;
		File file = new File(filePath);
		long fileSize=file.length();
		//System.out.println("Actula file size is "+fileSize);
		long recordsCount =  (fileSize/size);
		//System.out.println("Sorting of file size :"+sortFileSize); 
		logger.info("Sorting of file size :"+sortFileSize); 
		//System.out.println("Total number of records(lines) in the file:"+recordsCount); 
		logger.info("Total number of records(lines) in the file:"+recordsCount); 
		//long maxRecordCount = 2500000 ;
		long numberofChunks = recordsCount/maxRecordCount;
		long chunkSize = (maxRecordCount * 100)/(1000 * 1000);
		//System.out.println("Size of a chunk of file ( as per memory allocated to this program for execution) is " + chunkSize +"MB" );
		logger.info("Size of a chunk of file ( as per memory allocated to this program for execution) is " + chunkSize +"MB" );
		//System.out.println("Number of Chunks needed to sort the file is "+numberofChunks);
		logger.info("Number of Chunks needed to sort the file is "+numberofChunks);
		//System.out.println("Number of threads to sort each chunk is "+noOfThreads);
		logger.info("Number of threads to sort each chunk is "+noOfThreads);
		//System.out.println("Number of files = numberofChunks *  number of threads per each chunk");
		logger.info("Number of files = numberofChunks *  number of threads per each chunk");
		long numberofFiles = (numberofChunks * noOfThreads);
		
		String splitCommand = "split -n " + numberofFiles + " -d " + filePath + " tmpFile";
		Process p;
		long startTime = System.nanoTime();
		try {
			p = Runtime.getRuntime().exec(splitCommand);
			p.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("Thus, " + numberofFiles+" small(temporary) files are created");
		logger.info("Thus, " + numberofFiles+" small(temporary) files are created");
		//System.out.println("Total No of records processed in each file is "+(maxRecordCount/noOfThreads));
		logger.info("Total No of records processed in each file is "+(maxRecordCount/noOfThreads));
		long startPosition = 0;
		List<File> file_list = new ArrayList<>();
		////System.out.println(numberofFiles+"files created");
		//System.out.println("Sorting of files initialized!");
		logger.info("Sorting of files initialized!");
		int tempIndex = 0;
		try{
			for(int i=0;i<numberofChunks;i++){
				
				final CountDownLatch executionCompleted = new CountDownLatch(noOfThreads);
				////System.out.println("Sorting threads started");
				sortMultiThread threadSort[] = new sortMultiThread[noOfThreads];
				String tmpSortFilePath = "";
				for(int j=0;j<noOfThreads;j++){
					//System.out.println("Sorting file "+tempIndex);
					logger.info("Sorting file "+tempIndex);
					if(tempIndex<10)
						tmpSortFilePath = "tmpFile0"+tempIndex;
					else
						tmpSortFilePath = "tmpFile" + tempIndex;
					threadSort[j] = new sortMultiThread(tmpSortFilePath,tmpSortFilePath+"sort",executionCompleted);
					threadSort[j].start();
					tempIndex = tempIndex + 1;
				}
				/*for (int j = 0; j < noOfThreads; j++) {
					threadSort[j].join();
				}*/
				////System.out.println("one set of sorted files done");
				try
        		{
        			////System.out.println("Wait for all threads to be done");
            		executionCompleted.await();
            		////System.out.println("All over");
        		}
        		catch (InterruptedException e)
        		{
            		e.printStackTrace();
        		}
			}
		
		} catch(Exception e){
			e.printStackTrace();
		}
		//System.out.println("Sorting completed!");
		logger.info("Sorting completed!");
		//System.out.println("Initializing K way merge to merge the sorted files");
		logger.info("Initializing K way merge to merge the sorted files");
		for(int i=0;i<numberofFiles;i++){
			if(i<10)
				file_list.add(new File("tmpFile0" + i + "sort"));
			else
				file_list.add(new File("tmpFile" + i + "sort"));
		}
		
        //System.in.read();
		File outputFile = new File("FinalSortedFile");
		performKWayMergeSort(file_list, outputFile, (int)numberofFiles);
		long endTime = System.nanoTime();
		//System.out.println("K way merge is completed.");
		logger.info("K way merge is completed.");
		//System.out.println("Final output file is stored in the directory. The sorted file after merging is FinalSortedFile");
		logger.info("Final output file is stored in the directory. The sorted file after merging is FinalSortedFile");
		long output = endTime - startTime;
		long executionTimeInSec = ((output / 1000000))/1000;
        //System.out.println("Execution time in seconds: " + executionTimeInSec);
        logger.info("Execution time in seconds: " + executionTimeInSec);
        int fileSizeInGB = Integer.parseInt(sortFileSize.substring(0, sortFileSize.length() - 2));
        //System.out.println("Total Data read: " + (fileSizeInGB * 3) + "GB");
        logger.info("Total Data read: " + (fileSizeInGB * 3) + "GB");
        //System.out.println("Total Data write: " + (fileSizeInGB * 3) + "GB");
        logger.info("Total Data write: " + (fileSizeInGB * 3) + "GB");
        long totalIOInMB = (long)(fileSizeInGB * 2 * 1000);
        double throughput = (double)totalIOInMB / executionTimeInSec;
        //System.out.println("Throughput is calculated as  totalIOInMB / executionTimeInSec");
        logger.info("Throughput is calculated as  totalIOInMB / executionTimeInSec");
        //System.out.println("Throughput  for Thread: "+noOfThreads+ " in  MB/Sec: " + throughput);
        logger.info("Throughput  for Thread: "+noOfThreads+ " in  MB/Sec: " + throughput);

	}


}
