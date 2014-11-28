
/**
 * 2D Point class
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class Point2D {

	// to which cluster the point belong
	private int cluster;
	// coordinates of the point
	private double x;
	private double y;
	
	public Point2D(double x, double y) {
		this.x = x;
		this.y = y;
		this.cluster = -1;
	}
	
	public Point2D(double x, double y, int cluster) {
		this(x, y);
		this.cluster = cluster;
	}
	
	public Point2D(Point2D point) {
		this(point.x, point.y);
	}
	
	/**
	 * compute square distance to another point
	 * @param that
	 * @return square distance between two points
	 */
	public double distanceTo(Point2D that) {
		return Math.pow(this.x - that.x, 2) 
				+ Math.pow(this.y - that.y, 2);
	}
	
	public double getX() {
		return x;
	}
	
	public double getY() {
		return y;
	}
	
	public void setCluster(int cluster) {
		this.cluster = cluster;
	}
	
	public int getCluster() {
		return this.cluster;
	}

	@Override
	public String toString() {
		return "(" + this.x + ", " + this.y + ")";
	}
}
