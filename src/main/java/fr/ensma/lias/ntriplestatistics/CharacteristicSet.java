package fr.ensma.lias.ntriplestatistics;

import java.util.Map;

/**
 * @author Louise PARKIN
 */
public class CharacteristicSet {
	private Integer subjectNb;

	private Map<String,Integer> predicates;

	public CharacteristicSet(Integer subjNb, Map<String,Integer> predicateSet) {
		this.setSubjectNb(subjNb);
		this.setPredicates(predicateSet);
	}

	public Integer getSubjectNb() {
		return subjectNb;
	}

	public void setSubjectNb(Integer subjectNb) {
		this.subjectNb = subjectNb;
	}

	public Map<String,Integer> getPredicates() {
		return predicates;
	}

	public void setPredicates(Map<String,Integer> predicates) {
		this.predicates = predicates;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((predicates == null) ? 0 : predicates.hashCode())+((subjectNb == null) ? 0 : subjectNb.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CharacteristicSet other = (CharacteristicSet) obj;
		if (predicates == null) {
			if (other.getPredicates() != null)
				return false;
		} else if (!subjectNb.equals(other.getSubjectNb())||!predicates.equals(other.getPredicates()))
			return false;
		return true;
	}
}
