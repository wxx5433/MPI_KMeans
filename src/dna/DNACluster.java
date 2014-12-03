package dna;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class DNACluster implements Serializable {
	private static final long serialVersionUID = -8138233508587401002L;
	private List<DNAUnit> DNALists;
	private DNAUnit centroid;
	private int length;

	public DNACluster() {
		DNALists = new ArrayList<DNAUnit>();
	}

	public void setCentroid(DNAUnit centroid) {
		this.centroid = centroid;
	}

	public DNAUnit getCentroid() {
		return centroid;
	}

	public DNAUnit updateCentroid() {
		if (isEmpty()) {
			centroid = new DNAUnit("");
		} else {
			centroid = getCenterIDFromList();
		}
		return centroid;
	}

	private DNAUnit getCenterIDFromList() {
		String value = "";
		String data = "";
		for (int i = 0; i < this.length; i++) {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (DNAUnit dnaUnit : DNALists) {
				data = dnaUnit.getValue().charAt(i) + "";
				if (map.containsKey(data)) {
					map.put(data, map.get(data) + 1);
				} else {
					map.put(data, 0);
				}
			}
			int max = Integer.MIN_VALUE;
			for (Entry<String, Integer> entry : map.entrySet()) {
				if (entry.getValue() > max) {
					max = entry.getValue();
					data = entry.getKey();
				}
			}
			value = value + data;
		}
		return new DNAUnit(value);
	}

	public void addDNA(DNAUnit dna) {
		DNALists.add(dna);
		this.length = dna.getLength();
	}

	public void addAll(DNACluster cluster) {
		for (DNAUnit dna : cluster.getDNAs()) {
			DNALists.add(dna);
		}
	}

	public boolean isEmpty() {
		return DNALists.size() == 0;
	}

	public Iterable<DNAUnit> getDNAs() {
		return this.DNALists;
	}

	public void removeDNA(DNAUnit dna) {
		DNALists.remove(dna);
	}

	public void printCluster(int rank) {
		System.out.println("rank " + rank + " start print cluster ");
		for (DNAUnit dna : DNALists) {
			System.out.println(dna);
		}
	}
}
