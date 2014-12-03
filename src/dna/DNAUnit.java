package dna;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

public class DNAUnit implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8252747988835770899L;
	private int DNALength;
	private String value;
	private int cluster;

	public DNAUnit() {
		this.cluster = -1;
	}

	public DNAUnit(int DNALength) {
		this.DNALength = DNALength;
		this.cluster = -1;
	}

	public DNAUnit(String value) {
		this.DNALength = value.length();
		this.value = value;
		this.cluster = -1;
	}

	public int getDistance(DNAUnit compare) {
		int diff = 0;
		if (compare.getValue().length() != 0) {
			for (int i = 0; i < this.DNALength; i++) {
				if (value.charAt(i) != compare.getValue().charAt(i))
					++diff;
			}
		} else {
			diff = DNALength;
		}
		return diff;
	}

	public void generateValue() {
		this.value = "";
		Random rand = new Random();
		int pos = 0;
		String data = "";
		for (int i = 0; i < this.DNALength; i++) {
			pos = rand.nextInt(4);
			switch (pos) {
			case 0:
				data = "A";
				break;
			case 1:
				data = "C";
				break;
			case 2:
				data = "G";
				break;
			case 3:
				data = "T";
				break;
			default:
				break;
			}
			value = value + data;
		}
	}

	public void generateValue(String value) {
		this.value = "";
		Random rand = new Random();
		int pos = 0;
		String data = "";
		ArrayList<Integer> posList = new ArrayList<Integer>();
		int length = rand.nextInt(5);
		while (length == 0)
			length = rand.nextInt(5);
		for (int i = 0; i < length; ++i) {
			pos = rand.nextInt(this.DNALength + 1);
			posList.add(pos);
		}
		for (int i = 0; i < this.DNALength; i++) {
			data = value.charAt(i) + "";
			if (posList.contains(i)) {
				pos = rand.nextInt(4);
				switch (pos) {
				case 0:
					if (data.equals("A"))
						data = "C";
					else
						data = "A";
					break;
				case 1:
					if (data.equals("C"))
						data = "G";
					else
						data = "C";
					break;
				case 2:
					if (data.equals("G"))
						data = "T";
					else
						data = "G";
					break;
				case 3:
					if (data.equals("T"))
						data = "A";
					else
						data = "T";
					break;
				default:
					break;
				}
			}
			this.value = this.value + data;
		}
	}

	public String getValue() {
		return this.value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getLength() {
		return this.DNALength;
	}

	public int getCluster() {
		return cluster;
	}

	public void setCluster(int cluster) {
		this.cluster = cluster;
	}

}
