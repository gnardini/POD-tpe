package model;

public enum Condition {
    NO_DATA,
    EMPLOYED,
    UNEMPLOYED,
    INACTIVE;

    public static Condition fromInt(int i) {
        return values()[i];
    }
}
