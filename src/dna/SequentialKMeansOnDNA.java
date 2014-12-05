package dna;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Do K Means on DNA
 * 
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class SequentialKMeansOnDNA {

	// all data dnas
	private List<DNAUnit> dnas;
	// total data dna Number
	private int dnaNum;
	// cluster number
	private int k;
	// maximum iterations to run k means
	private int maxIter;
	// list of k clusters
	private List<DNACluster> dnaClusters;

	public SequentialKMeansOnDNA(String fileName, int k, int maxIter) {
		this.k = k;
		this.maxIter = maxIter;
		this.dnaClusters = new ArrayList<DNACluster>();
		this.dnas = new ArrayList<DNAUnit>();
		loadData(fileName);
		this.dnaNum = dnas.size();
		initializeCluster();
	}

	/**
	 * load all data dnas from CSV file
	 * 
	 * @param fileName
	 *            name of the CSV file to load
	 */
	private void loadData(String fileName) {
		System.out.println("load data");
		FileReader fw;
		try {
			fw = new FileReader(fileName);
			BufferedReader bw = new BufferedReader(fw);
			String line = "";
			while ((line = bw.readLine()) != null) {
				DNAUnit dnaUnit = new DNAUnit(line);
				dnas.add(dnaUnit);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Randomly choose k dnas to be the centroid dna
	 */
	private void initializeCluster() {
		assert (this.dnaNum >= k);

		// use hash set to avoid choosing the same dna
		Set<Integer> centerIndexes = new HashSet<Integer>();
		Random random = new Random();
		for (int i = 0; i < k;) {
			int centerIndex = random.nextInt(dnaNum);
			if (centerIndexes.contains(centerIndex)) {
				continue;
			}
			DNACluster pc = new DNACluster();
			pc.setCentroid(new DNAUnit(dnas.get(centerIndex).getValue()));
			dnaClusters.add(pc);
			centerIndexes.add(centerIndex);
			++i;
		}
	}

	/**
	 * Do Clustering all all data dnas using K Means algorithm Stop conditions:
	 * 1. reach max iterations 2. no change between 2 iterations
	 */
	public void doClustering() {
		boolean changed = true;
		for (int i = 0; i < maxIter; ++i) {
			System.out.println("Iteration " + (i + 1) + "...");
			for (DNAUnit dna : dnas) {
				int clusterIndex = findNearestCentroid(dna);
				DNACluster pc = dnaClusters.get(clusterIndex);
				int originalClusterIndex = dna.getCluster();
				// first iteration
				if (originalClusterIndex == -1) {
					pc.addDNA(dna);
					dna.setCluster(clusterIndex);
					continue;
				}
				// remove from the original cluster
				if (clusterIndex != originalClusterIndex) {
					dnaClusters.get(originalClusterIndex).removeDNA(dna);
					changed = true; // some dna change to another cluster
					// add to new cluster
					pc.addDNA(dna);
					dna.setCluster(clusterIndex);
				}
			}
			// no change between 2 iterations, already converge!
			if (!changed) {
				break;
			}
			// update cluster centroid
			updateCentroid();
			changed = false;
		}
	}

	/**
	 * update all clusters' centroid
	 */
	private void updateCentroid() {
		for (DNACluster pc : dnaClusters) {
			pc.updateCentroid();
		}
	}

	/**
	 * assign each dna to its nearest cluster centroid
	 * 
	 * @param dna
	 *            the dna data to be assigned
	 * @return index of the cluster in cluster list
	 */
	private int findNearestCentroid(DNAUnit dna) {
		int minDistance = Integer.MAX_VALUE;
		int minIndex = 0;
		for (int i = 0; i < k; ++i) {
			DNAUnit centroid = dnaClusters.get(i).getCentroid();
			int distance = dna.getDistance(centroid);
			if (distance < minDistance) {
				minDistance = distance;
				minIndex = i;
			}
		}
		return minIndex;
	}

	/**
	 * Write result to file
	 * 
	 * @param outputFileName
	 *            name of the output file
	 */
	public void outputResult(String outputFileName) {
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(outputFileName);
			bw = new BufferedWriter(fw);
			for (int i = 0; i < k; ++i) {
				DNACluster pc = dnaClusters.get(i);
				// System.out.println("Cluster " + i);
				bw.write("Cluster " + i + ":\n");
				for (DNAUnit dna : pc.getDNAs()) {
					// System.out.println("\t" + dna);
					bw.write("\t" + dna.getValue() + "\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
				fw.close();
			} catch (IOException e) {
				System.out.println("Fail to close output file");
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		int k = Integer.parseInt(args[0]);
		int maxIter = Integer.parseInt(args[1]);
		String inputFileName = args[2];
		String outputFileName = args[3];
		SequentialKMeansOnDNA kmp = new SequentialKMeansOnDNA(inputFileName, k,
				maxIter);
		kmp.doClustering();
		kmp.outputResult(outputFileName);
	}
}
