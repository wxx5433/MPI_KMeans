import java.util.HashSet;
import java.util.Set;


public class PointCluster {
	private Set<Point2D> points;
	private Point2D centroid;
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
		// what to do if there are no points in the cluster????
		
		
		int pointsNum = points.size();
		double sumX = 0, sumY = 0;
		for (Point2D point: points) {
			sumX += point.getX();
			sumY += point.getY();
		}
		centroid = new Point2D(sumX / pointsNum, sumY / pointsNum);
		return centroid;
	}
	
	public void addPoint(Point2D point) {
		points.add(point);
	}
	
	public void removePoint(Point2D point) {
		points.remove(point);
	}
	
	public boolean isEmpty() {
		return points.size() == 0;
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
