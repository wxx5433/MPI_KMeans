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

import mpi.MPI;

/**
 * Parallel version of K means on DNA.
 * 
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class ParallelKMeansOnDNA {

	// all DNA data
	private List<DNAUnit> allDNAData;
	// total DNA Number
	private int DNANum;
	// cluster number
	private int k;
	// maximum iterations to run k means
	private int maxIter;
	// list of k clusters
	private DNACluster[] DNAClusters;

	private DNAUnit[] centroids;

	private int rank;
	private int size;
	private int offset;
	private int len;

	public ParallelKMeansOnDNA(String fileName, int k, int maxIter) {
		this.rank = MPI.COMM_WORLD.Rank();
		this.size = MPI.COMM_WORLD.Size();
		this.k = k;
		this.maxIter = maxIter;
		this.allDNAData = new ArrayList<DNAUnit>();
		loadData(fileName);
		this.DNANum = allDNAData.size();
		this.centroids = new DNAUnit[k];
		this.len = DNANum / (size - 1);
		this.offset = len * (rank - 1);
		if (rank == 0) { // master initialize centroid dna
			initializeCluster();
		}
	}

	/**
	 * load all DNA data from CSV file
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
				allDNAData.add(dnaUnit);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Randomly choose k dnas to be the centroid dnas
	 */
	private void initializeCluster() {
		System.out.println("call init");
		assert (this.DNANum >= k);
		// use hash set to avoid choosing the same dna
		Set<Integer> centerIndexes = new HashSet<Integer>();
		Random random = new Random();
		for (int i = 0; i < k;) {
			int centerIndex = random.nextInt(DNANum);
			if (centerIndexes.contains(centerIndex)) {
				continue;
			}
			centroids[i++] = new DNAUnit(allDNAData.get(centerIndex).getValue());
			centerIndexes.add(centerIndex);
		}
	}

	/**
	 * Do Clustering all all DNA data using K Means algorithm Stop conditions:
	 * 1. reach max iterations 2. no change between 2 iterations
	 */
	public void doClustering() {
		if (rank == 0) { // master do not compute
			for (int iter = 1; iter < maxIter; ++iter) {
				// tell slaves the new centroid dnas
				broadcastNewCentroids();

				// ask each slaves to know if the algorithm can stop
				boolean stop = canStop();
				if (stop) { // done!
					return;
				}

				// aggregate all clusters info from slaves
				aggregateClustersInfo();

				// now can easily recalculate centroids by infomation fetched
				// from slaves
				updateCentroid();
			}
			// reach maximum iterations, stop the algorithm!
			tellStop(true);
		} else { // slaves
			int iter = 1;
			while (true) {
				System.out.println("Rank " + rank + " Iteration " + iter
						+ "...");
				// first receive new centroid dnas from master
				receiveNewCentroids();

				// assign each dna to its nearest centroid dna
				boolean[] changed = new boolean[1];
				DNACluster[] tmpClusters = new DNACluster[k];
				for (int i = 0; i < k; ++i) {
					tmpClusters[i] = new DNACluster();
				}
				changed[0] = computing(tmpClusters);

				// tell master if there is change between 2 iterations
				MPI.COMM_WORLD.Send(changed, 0, 1, MPI.BOOLEAN, 0, 2);

				// receive from master if slave should stop computing
				boolean[] stop = new boolean[1];
				MPI.COMM_WORLD.Recv(stop, 0, 1, MPI.BOOLEAN, 0, 3);
				// System.out.println("rank " + rank +
				// " received stop info from master " + stop[0]);
				if (stop[0]) { // done!
					System.out.println("rank " + rank + " finish computing!");
					break;
				}

				// if not done, tell master its clusters infomation
				MPI.COMM_WORLD.Send(tmpClusters, 0, k, MPI.OBJECT, 0, 4);
				// System.out.println("rank " + rank +
				// " sending cluster info to master");
				++iter;
			}
		}
	}

	/**
	 * Called by master. If all slaves' dnas do not change cluster during two
	 * consecutive iterations, the algorithm can stop. The master also tell all
	 * slaves the stop information
	 * 
	 * @return true if the algorithm can stop, false otherwise.
	 */
	private boolean canStop() {
		boolean changed = false;
		// receive from all slaves if their dnas have changed clusters between
		// 2 clusters
		for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
			// It's weird that I cannot simply pass boolean using
			// MPI.COMM_WORLD.Send
			boolean[] slaveChanged = new boolean[1];
			slaveChanged[0] = false;
			MPI.COMM_WORLD.Recv(slaveChanged, 0, 1, MPI.BOOLEAN, slaveRank, 2);
			changed |= slaveChanged[0];
			// System.out.println("Receive from rank " + rank +
			// " of changed info: " + slaveChanged[0]);
		}
		tellStop(!changed);
		return !changed;
	}

	/**
	 * Called by master to tell all slaves if they should stop computing
	 * 
	 * @param stopFlag
	 *            true to stop, false to continue
	 */
	private void tellStop(boolean stopFlag) {
		// tell all slaves if the algorithm can stop
		boolean[] stop = new boolean[1];
		stop[0] = stopFlag;
		for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
			MPI.COMM_WORLD.Send(stop, 0, 1, MPI.BOOLEAN, slaveRank, 3);
		}
	}

	/**
	 * Called by master to tell all slaves the new centroids
	 */
	private void broadcastNewCentroids() {
		for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
			// System.out.println("sending to rank " + slaveRank +
			// " new centoird dna");
			MPI.COMM_WORLD.Send(centroids, 0, k, MPI.OBJECT, slaveRank, 1);
		}
	}

	/**
	 * Called by slaves to receive new centroids from master
	 */
	private void receiveNewCentroids() {
		MPI.COMM_WORLD.Recv(centroids, 0, k, MPI.OBJECT, 0, 1);
		for (int i = 0; i < k; ++i) {
			System.out.println("rank " + rank + " receive centroid dna " + i
					+ ": " + centroids[i]);
		}
	}

	/**
	 * Called by slaves to compute 1. each DNA belong to which cluster 2. sum of
	 * the coordinates in all clusters (used by master to efficiently get new
	 * centroids)
	 * 
	 * @return
	 */
	private boolean computing(DNACluster[] tmpClusters) {
		boolean changed = false;
		int start = offset, end;
		if (rank == size - 1) { // last processor may have more dnas to
								// compute
			end = DNANum;
		} else {
			end = offset + len;
		}
		for (int index = start; index < end; ++index) {
			DNAUnit dna = allDNAData.get(index);
			int clusterIndex = findNearestCentroid(dna);
			DNACluster pc = tmpClusters[clusterIndex];
			int originalClusterIndex = dna.getCluster();
			// first iteration or change to another cluster
			if (originalClusterIndex == -1
					|| clusterIndex != originalClusterIndex) {
				changed = true;
				// pc.printCluster(rank);
			}
			pc.addDNA(dna);
			dna.setCluster(clusterIndex);
			// System.out.println("rank " + rank + " " + dna + " to cluster "
			// + clusterIndex);
		}
		return changed;
	}

	/**
	 * Called by master Each slave is assigned a number of dnas and compute to
	 * which cluster these dnas belong. Master then aggregate all this
	 * information.
	 */
	private void aggregateClustersInfo() {
		// each time we get latest info from slaves
		DNAClusters = new DNACluster[k];
		for (int i = 0; i < k; ++i) {
			DNAClusters[i] = new DNACluster();
		}
		for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
			DNACluster[] tmpClusters = new DNACluster[k];
			MPI.COMM_WORLD.Recv(tmpClusters, 0, k, MPI.OBJECT, slaveRank, 4);
			// System.out.println("Reveive clusters info from slave rank " +
			// slaveRank);
			for (int i = 0; i < k; ++i) {
				DNAClusters[i].addAll(tmpClusters[i]);
			}
		}
	}

	/**
	 * update all clusters' centroid dna
	 */
	private void updateCentroid() {
		for (int i = 0; i < k; ++i) {
			DNACluster pc = DNAClusters[i];
			centroids[i] = pc.updateCentroid();
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
			DNAUnit centroid = centroids[i];
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
		if (rank != 0) {
			return;
		}
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(outputFileName);
			bw = new BufferedWriter(fw);
			for (int i = 0; i < k; ++i) {
				DNACluster pc = DNAClusters[i];
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
		MPI.Init(args);
		// user arguments start from index 3
		int k = Integer.parseInt(args[3]);
		int maxIter = Integer.parseInt(args[4]);
		String inputFileName = args[5];
		String outputFileName = args[6];
		ParallelKMeansOnDNA kmp = new ParallelKMeansOnDNA(inputFileName, k,
				maxIter);
		kmp.doClustering();
		kmp.outputResult(outputFileName);
		MPI.Finalize();
	}
}
