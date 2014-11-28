import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

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
			System.out.println("Fail to open input csv file");
			e.printStackTrace();
		}
		br = new BufferedReader(fr);
	}
	
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
	
	// just a test
	public static void main(String[] args) {
		CSVReader csvReader = new CSVReader("cluster.csv");
		String[] resultStrings;
		while ((resultStrings = csvReader.readRecord()) != null) {
			for (String str: resultStrings) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
	}
}
