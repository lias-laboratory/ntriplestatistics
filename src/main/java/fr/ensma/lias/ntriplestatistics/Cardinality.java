package fr.ensma.lias.ntriplestatistics;

/**
 * @author Mickael BARON
 */
public class Cardinality {

	private Long max;

	private Long min;

	public Cardinality(Long min, Long max) {
		this.max = max;
		this.min = min;
	}

	public Long getMax() {
		return max;
	}

	public void setMax(Long max) {
		this.max = max;
	}

	public Long getMin() {
		return min;
	}

	public void setMin(Long min) {
		this.min = min;
	}
}
