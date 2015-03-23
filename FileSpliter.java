import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

			
	 public class FileSpliter {
	   
	    public List<String> splitFile(File file, Integer fileBlockSize, String tempFolder) throws IOException {
	    	ArrayList<String> fileNames = new ArrayList<String>();
	        FileInputStream fis = new FileInputStream(file);
	        try {
	            byte[] buffer = new byte[fileBlockSize];
	            int remaining = buffer.length; 
	            int blockNumber = 1;
	            while (true) {
	                int read = fis.read(buffer, buffer.length - remaining, remaining);
	                if (read >= 0) { // some bytes were read
	                    remaining -= read;
	                    if (remaining == 0) { // the buffer is full
	                    	String fileName = tempFolder + "/output_" + blockNumber + ".txt";
	                    	fileNames.add(fileName);
	                        writeBlock(blockNumber, buffer, buffer.length - remaining,fileName);
	                        blockNumber++;
	                        remaining = buffer.length;
	                    }
	                }
	                else { 
	                    // the end of the file was reached. If some bytes are in the buffer
	                    // they are written to the last output file
	                    if (remaining < buffer.length) {
	                    	String fileName = tempFolder + "/output_" + blockNumber + ".txt";
	                    	fileNames.add(fileName);
	                        writeBlock(blockNumber, buffer, buffer.length - remaining,fileName);
	                    }
	                    break;
	                }
	            }
	            return fileNames;
	        }
	        finally {
	            fis.close();
	        }
	    }

	    private void writeBlock(int blockNumber, byte[] buffer, int length,String fileName) throws IOException {
	        FileOutputStream fos = new FileOutputStream(fileName);
	        try {
	            fos.write(buffer, 0, length);
	        }
	        finally {
	            fos.close();
	        }
	    }
	}