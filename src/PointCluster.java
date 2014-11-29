import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import mpi.*;


public class PointCluster implements Serializable {
	private static final long serialVersionUID = -8138233508587401002L;
	private Set<Point2D> points;
	private Point2D centroid;
	private double sumX;
	private double sumY;
	//	private Point2D farthestPoint;
	//	private double farestDistance;

	public PointCluster() {
		points = new HashSet<Point2D>();
	}

	public void setCentroid(Point2D point) {
		this.centroid = point;
	}

	public Point2D getCentroid() {
		return centroid;
	}

	public Point2D updateCentroid() {
		if (isEmpty()) {
			centroid = new Point2D(0, 0);
		} else {
			int pointsNum = points.size();
//			double sumX = 0, sumY = 0;
//			for (Point2D point: points) {
//				sumX += point.getX();
//				sumY += point.getY();
//			}
			centroid = new Point2D(sumX / pointsNum, sumY / pointsNum);
		}
		return centroid;
	}
	
	public void increaseSum(double numX, double numY) {
		this.sumX += numX;
		this.sumY += numY;
	}
	
	public double getSumX() {
		return sumX;
	}
	
	public double getSumY() {
		return sumY;
	}

	public void addPoint(Point2D point) {
		points.add(point);
	}
	
	public void addPointAndIncreaseSum(Point2D point) {
		addPoint(point);
		increaseSum(point.getX(), point.getY());
	}
	
	public void addAll(PointCluster cluster) {
		for (Point2D point: cluster.getPoints()) {
			points.add(point);
		}
	}

	public void removePointAndDecreaseSum(Point2D point) {
		points.remove(point);
		sumX -= point.getX();
		sumY -= point.getY();
	}

	public boolean isEmpty() {
		return points.size() == 0;
	}

	public Iterable<Point2D> getPoints() {
		return this.points;
	}
	
	public void printCluster(int rank) {
		System.out.println("rank " + rank + " start print cluster ");
		for (Point2D point: points) {
			System.out.println(point);
		}
	}
	//
	//	public Point2D getFarthestPoint() {
	//		return farthestPoint;
	//	}
	//
	//	public void setFarthestPoint(Point2D farthestPoint) {
	//		this.farthestPoint = farthestPoint;
	//	}
	//
	//	public double getFarestDistance() {
	//		return farestDistance;
	//	}
	//
	//	public void setFarestDistance(double farestDistance) {
	//		this.farestDistance = farestDistance;
	//	}

}
