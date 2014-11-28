import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;


public class KMeansPoint {

	private List<Point2D> points;
	private int pointNum;
	private int k;
	private List<PointCluster> pointClusters;
	
	public KMeansPoint(String fileName, int k) {
		this.k = k;
		this.pointClusters = new ArrayList<PointCluster>();
		this.points = new ArrayList<Point2D>();
		loadData(fileName);
		this.pointNum = points.size();
		initializeCluster();
	}
	
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
	 * need iterations here !!!!!!!!!!!
	 */
	public void doClustering() {
		for (Point2D point: points) {
			int clusterIndex = findNearestCentroid(point);
			PointCluster pc = pointClusters.get(clusterIndex);
			int originalClusterIndex = point.getCluster();
			// remove from the original cluster
			if (originalClusterIndex != -1 
					&& clusterIndex != originalClusterIndex) {
				pointClusters.get(originalClusterIndex).removePoint(point);
			}
			// add to new cluster
			pc.addPoint(point);
		}
		updateCentroid();
	}
	
	private void updateCentroid() {
		for (PointCluster pc: pointClusters) {
			pc.updateCentroid();
		}
	}
	
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
	
	public static void main(String[] args) {
		int k = Integer.parseInt(args[0]);
		KMeansPoint kmp = new KMeansPoint("cluster.csv", k);
		kmp.doClustering();
	}

}
