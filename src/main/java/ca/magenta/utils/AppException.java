package ca.magenta.utils;


public class AppException extends java.lang.Exception {

    private static final long serialVersionUID = -6049895267370229367L;

    public AppException(String problemDetail) {
        super(problemDetail);
    }

    public AppException(String problemDetail, Throwable cause) {
        super(problemDetail, cause);
    }

    public AppException(Throwable e) {
        super(e.getClass().getSimpleName(), e);
    }
}
