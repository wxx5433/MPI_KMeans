import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import mpi.*;

/**
 * Read CSV file, results is put in a String array.
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class CSVReader {
	private String fileName;
	private FileReader fr;
	private BufferedReader br;
	private final static String separator = ",";
	
	public CSVReader(String fileName) {
		this.fileName = fileName;
		openFile();
	}
	
	private void openFile() {
		try {
			fr = new FileReader(fileName);
		} catch (FileNotFoundException e) {
			System.out.println(fileName);
			System.out.println("Fail to open input csv file");
			e.printStackTrace();
		}
		br = new BufferedReader(fr);
	}
	
	/**
	 * Treat one line in the CSV file as one record, 
	 * use "," to separate fields in the record
	 * @return String array of all the fields in the record
	 */
	public String[] readRecord() {
		StringTokenizer tokens = null;
		String[] results = null;
		try {
			String line = br.readLine();
			if (line == null) {
				closeFile();
				return null;
			}
			tokens = new StringTokenizer(line, separator);
			int tokenNum = tokens.countTokens();
			results = new String[tokenNum];
			for (int i = 0; i < tokenNum; ++i) {
				results[i] = tokens.nextToken();
			}
		} catch (IOException e) {
			closeFile();
			return null;
		}
		return results;
	}
	
	private void closeFile() {
		try {
			br.close();
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
