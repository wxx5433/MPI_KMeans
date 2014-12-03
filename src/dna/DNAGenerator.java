package dna;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class DNAGenerator {

	int numClusters;
	int numDNAs;
	int DNALength;
	String outputPath;
	ArrayList<DNAUnit> centerIDs = new ArrayList<DNAUnit>();
	ArrayList<DNAUnit> dnas = new ArrayList<DNAUnit>();

	public DNAGenerator(int numClusters, int numDNAs, int DNALength,
			String outputPath) {
		this.numClusters = numClusters;
		this.numDNAs = numDNAs;
		this.DNALength = DNALength;
		this.outputPath = outputPath;
	}

	public void start() {
		generateCenterIDs();
		printCenterIDs();
		generateDNAStrides();
		printAllDNA();
		writeData();
	}

	private void printCenterIDs() {
		for (DNAUnit dnaUnit : centerIDs) {
			System.out.println(dnaUnit.getValue());
		}
	}

	private void printAllDNA() {
		for (DNAUnit dnaUnit : dnas) {
			System.out.println(dnaUnit.getValue() + "---");
		}
	}

	private void generateCenterIDs() {
		for (int i = 0; i < numClusters; ++i) {
			DNAUnit centerID = new DNAUnit(DNALength);
			boolean flag = false;
			while (!flag) {
				centerID.generateValue();
				if (centerIDs.size() == 0) {
					flag = true;
				}
				for (DNAUnit dnaUnit : centerIDs) {
					flag = true;
					if (dnaUnit.getDistance(centerID) < DNALength - 3) {
						System.out.println("distance:"
								+ dnaUnit.getDistance(centerID));
						System.out.println("DNALength: " + DNALength);
						flag = false;
						break;
					}
				}
			}
			centerIDs.add(centerID);
		}
	}

	private void generateDNAStrides() {
		for (DNAUnit dnaUnit : centerIDs) {
			dnas.add(dnaUnit);
			for (int i = 0; i < numDNAs - 1; i++) {
				DNAUnit dnaData = new DNAUnit(this.DNALength);
				dnaData.generateValue(dnaUnit.getValue());
				dnas.add(dnaData);
			}
		}
	}

	private void writeData() {
		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter(outputPath);
			bw = new BufferedWriter(fw);
			for (DNAUnit dnaUnit : dnas) {
				bw.write(dnaUnit.getValue());
				bw.write("\n");
			}
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		if (args.length != 4) {
			System.out
					.println("please input 4 arguments: NumberOfClusters NumberOfDNAsEachCluster LengthOfDNA outputPath");
			System.exit(-1);
		}
		int numClusters = Integer.parseInt(args[0]);
		int numDNAs = Integer.parseInt(args[1]);
		int DNALength = Integer.parseInt(args[2]);
		String outputPath = args[3];
		DNAGenerator dnaGenerator = new DNAGenerator(numClusters, numDNAs,
				DNALength, outputPath);
		dnaGenerator.start();
	}

}
