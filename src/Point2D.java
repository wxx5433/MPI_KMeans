
/**
 * 2D Point class
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 *
 */
public class Point2D {

	// to which cluster the point belong
	private int cluster;
	private double x;
	private double y;
	
	public Point2D(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	public Point2D(double x, double y, int cluster) {
		this(x, y);
		this.cluster = cluster;
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
}
