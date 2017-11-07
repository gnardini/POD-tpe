package model;

import java.io.Serializable;

/**
 * Contains basic information about the population of a department.
 */
public class DepartmentPopulation implements Serializable, Comparable<DepartmentPopulation> {

    private static final long serialVersionUID = 1L;

    private String department;
    private Long population;

    @Override
    public int compareTo(DepartmentPopulation o) {
        return o.population.compareTo(population);
    }

    public DepartmentPopulation(String department, Long population) {
        this.department = department;
        this.population = population;
    }

    public String getDepartment() {
        return department;
    }

    public Long getPopulation() {
        return population;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DepartmentPopulation that = (DepartmentPopulation) o;

        return department.equals(that.department);
    }

    @Override
    public int hashCode() {
        return department.hashCode();
    }

    @Override
    public String toString() {
        return getDepartment() + "," + getPopulation();
    }

}
