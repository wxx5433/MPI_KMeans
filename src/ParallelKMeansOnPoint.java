import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import mpi.*;

// Send(java.lang.Object buf, int offset, int count, Datatype datatype, int dest, int tag)
// Blocking send operation.
public class ParallelKMeansOnPoint {

	// all data points
	private List<Point2D> allDataPoints;
	// total data point Number
	private int pointNum;
	// cluster number
	private int k;
	// maximum iterations to run k means
	private int maxIter;
	// list of k clusters
	private PointCluster[] pointClusters;

	private Point2D[] centroids;

	private int rank;
	private int size;
	private int offset;
	private int len;

	public ParallelKMeansOnPoint(String fileName, int k, int maxIter) {
		this.rank = MPI.COMM_WORLD.Rank();
		this.size = MPI.COMM_WORLD.Size();
		this.k = k;
		this.maxIter = maxIter;
//		this.pointClusters = new PointCluster[k];
		this.allDataPoints = new ArrayList<Point2D>();
		loadData(fileName);
		this.pointNum = allDataPoints.size();
		this.centroids = new Point2D[k];
		this.len = pointNum / (size - 1);
		this.offset = len * (rank - 1);
		if (rank == 0) {   // master initialize centroid points
			initializeCluster();
		}
	}

	/**
	 * load all data points from CSV file
	 * @param fileName name of the CSV file to load
	 */
	private void loadData(String fileName) {
		System.out.println("load data");
		CSVReader csvReader = new CSVReader(fileName);
		String[] coordinates = null;
		while ((coordinates = csvReader.readRecord()) != null) {
			assert(coordinates.length == 2);
			Point2D point = new Point2D(
					Double.parseDouble(coordinates[0]), 
					Double.parseDouble(coordinates[1]));
			allDataPoints.add(point);
		}
	}

	/**
	 * Randomly choose k points to be the centroid point
	 */
	private void initializeCluster() {
		System.out.println("call init");
		assert(this.pointNum >= k);
		//			System.out.println("I'm rank 0!!");
		// use hash set to avoid choosing the same point
		centroids[0] = new Point2D(allDataPoints.get(0));
		centroids[1] = new Point2D(allDataPoints.get(3));
		System.out.println(centroids[0]);
		System.out.println(centroids[1]);
//		Set<Integer> centerIndexes = new HashSet<Integer>();
//		Random random = new Random();
//		for (int i = 0; i < k;) {
//			int centerIndex = random.nextInt(pointNum);
//			if (centerIndexes.contains(centerIndex)) {
//				continue;
//			}
//			centroids[i++] = new Point2D(allDataPoints.get(centerIndex));
//			centerIndexes.add(centerIndex);
//		} 
		//			System.out.println("size " + size);
	}

	/**
	 * Do Clustering all all data points using K Means algorithm
	 * Stop conditions: 
	 * 		1. reach max iterations 
	 *  	2. no change between 2 iterations
	 */
	public void doClustering() {
		if (rank == 0) { // rank 0 do not do compute
			for (int iter = 1; iter < maxIter; ++iter) {
				// tell slaves the new centroid points
				for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
					System.out.println("sending to rank " + slaveRank + " new centoird point");
					MPI.COMM_WORLD.Send(centroids, 0, k, MPI.OBJECT, slaveRank, 1);
				}
				boolean changed = false;
				// receive from all slaves if their points have changed clusters between 2 clusters
				for (int slaveRank = 1; slaveRank < size; ++slaveRank ) {
					// It's weird that I cannot simply pass boolean using MPI.COMM_WORLD.Send
					boolean[] slaveChanged = new boolean[1];
					slaveChanged[0] = false;
					MPI.COMM_WORLD.Recv(slaveChanged, 0, 1, MPI.BOOLEAN, slaveRank, 2);
					changed |= slaveChanged[0];
					System.out.println("Receive from rank " + rank + " of changed info: " + slaveChanged[0]);
				}
				boolean[] stop = new boolean[1];
				stop[0] = !changed;
				// if no change through all slaves, then ask slaves to stop computing
				for (int slaveRank = 1; slaveRank < size; ++slaveRank ) {
					MPI.COMM_WORLD.Send(stop, 0, 1, MPI.BOOLEAN, slaveRank, 3);
				}

				if (stop[0]) {
					System.out.println("telling slaves to stop");
					break;
				}
				// aggregate all clusters info from slaves
				pointClusters = new PointCluster[k];
				for (int i = 0; i < k; ++i) {
					pointClusters[i] = new PointCluster();
				}
//				for (int i = 0; i < k; ++i) {
//					pointClusters[i].printCluster(rank);
//				}

				for (int slaveRank = 1; slaveRank < size; ++slaveRank) {
					PointCluster[] tmpClusters = new PointCluster[k];
					MPI.COMM_WORLD.Recv(tmpClusters, 0, k, MPI.OBJECT, slaveRank, 4);
					System.out.println("Reveive clusters info from slave rank " + slaveRank);
					for (int i = 0; i < k; ++i) {
//						tmpClusters[i].printCluster(rank);
						pointClusters[i].addAll(tmpClusters[i]);  // do not calculate sum here.
						// add clusters sum computed by slave ranks
						pointClusters[i].increaseSum(tmpClusters[i].getSumX(), tmpClusters[i].getSumY());
//						pointClusters[i].printCluster(rank);
						System.out.println("sumX: " + pointClusters[i].getSumX() + " sumY:" + pointClusters[i].getSumY());
					}
				}
				outputResult("tmp" + iter);
				// now can easily recalculate centroids by infomation fetched from slaves
				updateCentroid();
			}
		} else {
			int iter = 1;
			while (true) {
				System.out.println("Rank " + rank + " Iteration " + iter + "...");
				// first receive new centroid points from master
				MPI.COMM_WORLD.Recv(centroids, 0, k, MPI.OBJECT, 0, 1);
				for (int i = 0; i < k; ++i) {
					System.out.println("rank " + rank + " receive centroid point: " 
							+ centroids[i]);
				}
				// assign each point to its nearest centroid point
				boolean[] changed = new boolean[1];
				changed[0] = false;
				int start = offset, end; 
				if (rank == size - 1) {  // last processor may have more points to compute
					end = pointNum;
				} else {
					end = offset + len;
				}
				System.out.println("rank " + rank + " start:" + start + ", end:" + end);
				PointCluster[] tmpClusters = new PointCluster[k];
				for (int i = 0; i < k; ++i) {
					tmpClusters[i] = new PointCluster();
				}
				for (int i = 0; i < k; ++i) {
					System.out.println("rank" + rank + " before computing");
					tmpClusters[i].printCluster(rank);
				}
				for (int index = start; index < end; ++index) {
					System.out.println("index " + index);
					Point2D point = allDataPoints.get(index);
					int clusterIndex = findNearestCentroid(point);
					PointCluster pc = tmpClusters[clusterIndex];
					int originalClusterIndex = point.getCluster();
					// first iteration or change to another cluster
					if (originalClusterIndex == -1 || clusterIndex != originalClusterIndex) {
						changed[0] = true;
//						System.out.println("rank " + rank + " " + point + " to cluster " + clusterIndex);
//						System.out.println("rank " + rank + " first iteration!");
//						System.out.print("Rank " + rank + " In computing: ");
//						pc.printCluster(rank);
					}
					pc.addPointAndIncreaseSum(point);
					point.setCluster(clusterIndex);
					System.out.println("rank " + rank + " " + point + " to cluster " + clusterIndex);
				}
				// tell master if there is change between 2 iterations
				System.out.println("rank " + rank + " sending changed info to master");
				MPI.COMM_WORLD.Send(changed, 0, 1, MPI.BOOLEAN, 0, 2);
				// receive from master if slave should stop computing
				boolean[] stop = new boolean[1];
				MPI.COMM_WORLD.Recv(stop, 0, 1, MPI.BOOLEAN, 0, 3);
				System.out.println("rank " + rank + " received stop info from master " + stop[0]);
				if (stop[0]) {  // done!
					System.out.println("rank " + rank + " finish computing!");
					break;
				}
				// if not done, tell master its clusters infomation
				System.out.println("before sending to master");
				for (int i = 0; i < k; ++i) {
					System.out.println("cluster " + i);
					tmpClusters[i].printCluster(rank);
				}
				MPI.COMM_WORLD.Send(tmpClusters, 0, k, MPI.OBJECT, 0, 4);
				System.out.println("rank " + rank + " sending cluster info to master");
				++iter;
			}
		}
	}

	/**
	 * update all clusters' centroid point
	 */
	private void updateCentroid() {
		for (int i = 0; i < k; ++i) {
			PointCluster pc = pointClusters[i];
			System.out.println("haha sum " + pc.getSumX() + ", " + pc.getSumY());
			centroids[i] = pc.updateCentroid();
		}
	}

	/**
	 * assign each point to its nearest cluster centroid
	 * @param point the data point to be assigned
	 * @return index of the cluster in cluster list
	 */
	private int findNearestCentroid(Point2D point) {
		double minDistance = Double.MAX_VALUE;
		int minIndex = 0;
		for (int i = 0; i < k; ++i) {
			Point2D centroid = centroids[i];
			double distance = point.distanceTo(centroid);
			if (distance < minDistance) {
				minDistance = distance;
				minIndex = i;
			}
		}
		return minIndex;
	}

	/**
	 * Write result to file
	 * @param outputFileName name of the output file
	 */
	public void outputResult(String outputFileName) {
		if (rank != 0) {
			return;
		}
		FileWriter fw  = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(outputFileName);
			bw = new BufferedWriter(fw);
			for (int i = 0; i < k; ++i) {
				PointCluster pc = pointClusters[i];

				System.out.println("Cluster " + i);
				bw.write("Cluster " + i + ":\n");
				for (Point2D point: pc.getPoints()) {
					System.out.println("\t" + point);
					bw.write("\t" + point + "\n");
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
		ParallelKMeansOnPoint kmp = new ParallelKMeansOnPoint(inputFileName, k, maxIter);
		kmp.doClustering();
		kmp.outputResult(outputFileName);
		MPI.Finalize();
	}
}
