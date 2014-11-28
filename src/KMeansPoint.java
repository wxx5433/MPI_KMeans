import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Do K Means on 2D points 
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class KMeansPoint {

	// all data points
	private List<Point2D> points;
	// total data point Number
	private int pointNum;
	// cluster number
	private int k;
	// maximum iterations to run k means
	private int maxIter;
	// list of k clusters
	private List<PointCluster> pointClusters;

	public KMeansPoint(String fileName, int k, int maxIter) {
		this.k = k;
		this.maxIter = maxIter;
		this.pointClusters = new ArrayList<PointCluster>();
		this.points = new ArrayList<Point2D>();
		loadData(fileName);
		this.pointNum = points.size();
		initializeCluster();
	}

	/**
	 * load all data points from CSV file
	 * @param fileName name of the CSV file to load
	 */
	private void loadData(String fileName) {
		CSVReader csvReader = new CSVReader(fileName);
		String[] coordinates = null;
		while ((coordinates = csvReader.readRecord()) != null) {
			assert(coordinates.length == 2);
			Point2D point = new Point2D(
					Double.parseDouble(coordinates[0]), 
					Double.parseDouble(coordinates[1]));
			points.add(point);
		}
	}

	/**
	 * Randomly choose k points to be the centroid point
	 */
	private void initializeCluster() {
		assert(this.pointNum >= k);
		// use hash set to avoid choosing the same point
		Set<Integer> centerIndexes = new HashSet<Integer>();
		Random random = new Random();
		for (int i = 0; i < k;) {
			int centerIndex = random.nextInt(pointNum);
			if (centerIndexes.contains(centerIndex)) {
				continue;
			}
			PointCluster pc = new PointCluster();
			pc.setCentroid(new Point2D(points.get(centerIndex)));
			pointClusters.add(pc);
			centerIndexes.add(centerIndex);
			++i;
		}
	}

	/**
	 * Do Clustering all all data points using K Means algorithm
	 * Stop conditions: 
	 * 		1. reach max iterations 
	 *  	2. no change between 2 iterations
	 */
	public void doClustering() {
		boolean changed = true;
		for (int i = 0; i < maxIter; ++i) {
			System.out.println("Iteration " + (i + 1) + "...");
			for (Point2D point: points) {
				int clusterIndex = findNearestCentroid(point);
				PointCluster pc = pointClusters.get(clusterIndex);
				int originalClusterIndex = point.getCluster();
				// first iteration
				if (originalClusterIndex == -1) {
					pc.addPoint(point);
					point.setCluster(clusterIndex);
					continue;
				}
				// remove from the original cluster
				if (clusterIndex != originalClusterIndex) {
					pointClusters.get(originalClusterIndex).removePoint(point);
					changed = true;  // some point change to another cluster
					// add to new cluster
					pc.addPoint(point);
					point.setCluster(clusterIndex);
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
	 * update all clusters' centroid point
	 */
	private void updateCentroid() {
		for (PointCluster pc: pointClusters) {
			pc.updateCentroid();
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
			Point2D centroid = pointClusters.get(i).getCentroid();
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
		FileWriter fw  = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(outputFileName);
			bw = new BufferedWriter(fw);
			for (int i = 0; i < k; ++i) {
				PointCluster pc = pointClusters.get(i);
				bw.write("Cluster " + i + ":\n");
				for (Point2D point: pc.getPoints()) {
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
		int k = Integer.parseInt(args[0]);
		int maxIter = Integer.parseInt(args[1]);
		String inputFileName = args[2];
		String outputFileName = args[3];
		KMeansPoint kmp = new KMeansPoint(inputFileName, k, maxIter);
		kmp.doClustering();
		kmp.outputResult(outputFileName);
	}
}

