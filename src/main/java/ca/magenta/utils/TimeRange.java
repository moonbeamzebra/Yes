package ca.magenta.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeRange {

    public TimeRange(long olderTime, long newerTime) {
        this.olderTime = olderTime;
        this.newerTime = newerTime;
    }

    private static final long MILLISECONDS_IN_1_MILLISECOND = 1;
    private static final long MILLISECONDS_IN_1_SECOND = 1000;
    private static final long MILLISECONDS_IN_1_MINUTE = 60 * MILLISECONDS_IN_1_SECOND;
    private static final long MILLISECONDS_IN_1_HOUR = 60 * MILLISECONDS_IN_1_MINUTE;
    private static final long MILLISECONDS_IN_1_DAY = 24 * MILLISECONDS_IN_1_HOUR;
    private static final long MILLISECONDS_IN_1_WEEK = 7 * MILLISECONDS_IN_1_DAY;
    private static final long MILLISECONDS_IN_1_MONTH = 30 * MILLISECONDS_IN_1_DAY;
    private static final long MILLISECONDS_IN_1_YEAR = 365 * MILLISECONDS_IN_1_DAY;

    private final long olderTime;
    private final long newerTime;

    private static final Pattern typeLastPattern = Pattern.compile("(\\d+)([yMdHmsSw])");

    // FROM1483104805352-TO1483105405352
    // FROM2016-12-30T08:33:25.352-TO2016-12-30T08:43:25.352
    private static final Pattern typeFromToPattern = Pattern.compile("FROM(.+)-TO(.+)");
    private static final SimpleDateFormat typeFromToSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    static public TimeRange returnTimeRangeBackwardFromNow(String periodString) throws AppException {
        periodString = periodString.trim();

        if (periodString.toUpperCase().startsWith("LAST")) {
            return returnTimeRange(true, System.currentTimeMillis(), periodString);
        } else if (periodString.toUpperCase().startsWith("FROM")) {
            return returnTime_FromTo(periodString.toUpperCase());
        }

        return returnTimeRange(true, System.currentTimeMillis(), periodString);
    }

    private static TimeRange returnTimeRange(boolean backward /* in the pass */,
                                             long offset,
                                             String periodString) throws AppException {

        long time = 0;

        periodString = periodString.trim();

        if (periodString.toUpperCase().startsWith("LAST")) {
            time = returnTime_LAST(periodString.substring(4));
        }

        long olderTime;
        long newerTime;
        if (backward) {
            newerTime = offset;
            olderTime = newerTime - time;
        } else // forward - in the future
        {
            olderTime = offset;
            newerTime = olderTime + time;
        }

        if ((olderTime >= 0) && (newerTime >= 0)) {
            return new TimeRange(olderTime, newerTime);
        } else {
            throw new AppException(String.format("Bad parameters: backward [%b],offset[%d],periodString[%s]; " +
                            "gives negative values: olderTime[%d], newerTime[%d]",
                    backward,
                    offset,
                    periodString,
                    olderTime,
                    newerTime));
        }
    }

    private static TimeRange returnTime_FromTo(String periodString) throws AppException {

        Matcher m = typeFromToPattern.matcher(periodString.trim());

        if (m.find()) {

            long fromPart = returnEpoch(m.group(1));
            long toPart = returnEpoch(m.group(2));

            return new TimeRange(fromPart, toPart);

        } else {
            throw new AppException(String.format("Bad type LAST periodString: [%s]", periodString));
        }

    }

    private static long returnEpoch(String timeString) throws AppException {

        // try epoch format
        try {
            return Long.valueOf(timeString);
        } catch (NumberFormatException e) {
            // Do nothing.  Now try formatted date way
        }

        // try yyyy-MM-dd'T'HH:mm:ss.SSS format
        try {
            return typeFromToSimpleDateFormat.parse(timeString).getTime();
        } catch (ParseException e) {
            // Do nothing.  Throw below (can add other acceptable format below later on)
        }

        // If we land here, we got no luck with parsing
        throw new AppException(String.format("Unable to parse FromTo type of date [%s]", timeString));

    }

    private static long returnTime_LAST(String periodString) throws AppException {

        Matcher m = typeLastPattern.matcher(periodString.trim());

        long multiplier;
        char periodSpecifier;

        if (m.find()) {

            multiplier = Long.valueOf(m.group(1));
            periodSpecifier = m.group(2).charAt(0);
            long time;

            switch (periodSpecifier) {
                // yyyy-MM-dd'T'HH:mm:ss.SSSZ
                //YEAR
                case 'y':
                    time = MILLISECONDS_IN_1_YEAR;
                    break;
                // MONTH
                case 'M':
                    time = MILLISECONDS_IN_1_MONTH;
                    break;
                // WEEK
                case 'w':
                    time = MILLISECONDS_IN_1_WEEK;
                    break;
                // DAY
                case 'd':
                    time = MILLISECONDS_IN_1_DAY;
                    break;
                // HOUR
                case 'H':
                    time = MILLISECONDS_IN_1_HOUR;
                    break;
                // MINUTE
                case 'm':
                    time = MILLISECONDS_IN_1_MINUTE;
                    break;
                // SECOND
                case 's':
                    time = MILLISECONDS_IN_1_SECOND;
                    break;
                // MILLISECOND
                case 'S':
                    time = MILLISECONDS_IN_1_MILLISECOND;
                    break;

                default:
                    throw new AppException("ASSERT FAILED");
            }

            return multiplier * time;

        } else {
            throw new AppException(String.format("Bad type LAST periodString: [%s]", periodString));
        }
    }

    public long getOlderTime() {
        return olderTime;
    }

    public long getNewerTime() {
        return newerTime;
    }

    @Override
    public String toString() {

        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz");
        String olderTimeStr = DATE_FORMAT.format(olderTime);
        String newerTimeStr = DATE_FORMAT.format(newerTime);

        return String.format("TimeRange{olderTime=%s(%s), newerTime=%s(%s)}",
                olderTimeStr,
                olderTime,
                newerTimeStr,
                newerTime);
    }
}
