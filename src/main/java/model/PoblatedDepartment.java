package model;

import java.io.Serializable;

public class PoblatedDepartment implements Serializable, Comparable<PoblatedDepartment> {

    private static final long serialVersionUID = 1L;

    private String department;
    private Long poblation;

    @Override
    public int compareTo(PoblatedDepartment o) {
        return o.poblation.compareTo(poblation);
    }

    public PoblatedDepartment(String department, Long poblation) {
        this.department = department;
        this.poblation = poblation;
    }

    public String getDepartment() {
        return department;
    }

    public Long getPoblation() {
        return poblation;
    }

    public synchronized void addPoblation(Long poblation) {
        this.poblation += poblation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PoblatedDepartment that = (PoblatedDepartment) o;

        return department.equals(that.department);
    }

    @Override
    public int hashCode() {
        return department.hashCode();
    }

    @Override
    public String toString() {
        return getDepartment() + "," + getPoblation();
    }

}
