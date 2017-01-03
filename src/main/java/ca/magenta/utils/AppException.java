package ca.magenta.utils;


/**
 * Generic AppException exception.
 *
 * @author jplaberge@magenta.ca
 */
public class AppException extends java.lang.Exception {

    private static final long serialVersionUID = -6049895267370229367L;

    public AppException(String problemDetail) {
        super(problemDetail.toString());
    }

    public AppException(String problemDetail, Throwable cause) {
        super(problemDetail.toString(), cause);
    }

}
