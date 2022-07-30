package domain;

import java.io.Serializable;
import java.util.Objects;

public class Department implements Serializable {

    private int deptId;
    private String deptName;

    public int getDeptId() {
        return deptId;
    }

    public void setDeptId(int deptId) {
        this.deptId = deptId;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Department that = (Department) o;
        return deptId == that.deptId && Objects.equals(deptName, that.deptName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deptId, deptName);
    }
}
