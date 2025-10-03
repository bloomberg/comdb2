/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translations from C structs to Java classes.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 * @author Sebastien Blind
 */
public class Cdb2Types {

    private static Logger logger = LoggerFactory.getLogger(Cdb2Types.class);

    /* Integer - (u_)short, (u_)int, longlong */
    public static class Int64 implements ByteArray {

        long value;

        public Int64() {
        /* default constructor */
        }

        public Int64(long value) {
            this.value = value;
        }

        public Int64(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(value);
            return bb.array();
        }

        @Override public int capacity() {
            return 8;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {

            if (bytes.length < 8)
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            value = bb.getLong();
            return true;
        }

        @Override public String toString() {
            return String.valueOf(value);
        }

        @Override public boolean toBool() {
            return value == 0 ? false : true;
        }

        @Override public long toLong() {
            return value;
        }

        @Override public double toDouble() {
            return (double) value;
        }

    }

    /* Real - float, double */
    public static class Real implements ByteArray {

        double value;

        public Real() {
        /* default constructor */
        }

        public Real(double value) {
            this.value = value;
        }

        public Real(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putDouble(value);
            return bb.array();
        }

        @Override public int capacity() {
            return 8;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            if (bytes.length < 8)
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            value = bb.getDouble();
            return true;
        }

        @Override public String toString() {
            return String.valueOf(value);
        }

        @Override public boolean toBool() {
            return value == 0 ? false : true;
        }

        @Override public long toLong() {
            return (long) value;
        }

        @Override public double toDouble() {
            return value;
        }

    }

    /* String - cstring, vutf8 */
    public static class CString implements ByteArray {

        byte[] bytes;

        public CString() {
        /* default constructor */
        }

        public CString(byte[] bytes) {
            this.bytes = bytes;
        }

        public CString(String string) {
            bytes = string.getBytes();
        }

        @Override public byte[] toBytes() {
            return bytes;
        }

        @Override public int capacity() {
            return bytes == null ? 0 : bytes.length;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            this.bytes = null;
            this.bytes = bytes;
            return true;
        }

        @Override public String toString() {
            if (bytes.length == 0)
                return null;
            return new String(bytes, 0, bytes.length - 1);
        }

        @Override public boolean toBool() {
            String str = toString();
            if ("1".equals(str) || "true".equalsIgnoreCase(str))
                return true;
            return false;
        }

        @Override public long toLong() {
            return Long.parseLong(toString());
        }

        @Override public double toDouble() {
            return Double.parseDouble(toString());
        }
    }

    /* Blob - byte[], blob */
    public static class Blob implements ByteArray {

        byte[] bytes;

        public Blob() {
        /* default constructor */
        }

        public Blob(byte[] bytes) {
            this.bytes = bytes;
        }

        public Blob(java.sql.Blob blob) {
            try {
                this.bytes = blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                logger.error("Unable to convert java.sql.Blob to comdb2 blob", e);
            }
        }

        @Override public byte[] toBytes() {
            return bytes;
        }

        @Override public int capacity() {
            return bytes == null ? 0 : bytes.length;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            this.bytes = null;
            this.bytes = bytes;
            return true;
        }

        @Override public String toString() {
            return new String(bytes);
        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Blob", "boolean");
        }

        @Override public long toLong() {
            throw new UnsupportedConversionException("Comdb2 Blob", "long");
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Blob", "double");
        }

    }

    /* Datetime - datetime */
    public static class Datetime implements ByteArray {
        int tm_sec;
        int tm_min;
        int tm_hour;
        int tm_mday;
        int tm_mon;
        int tm_year;
        int tm_wday;
        int tm_yday;
        int tm_isdst;

        int msec;
        String tzname;

        public final static int MAXT_TZ_LEN = 36;

        public Datetime() {
        /* default constructor */
        }

        public Datetime(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        public Datetime(int tm_sec, int tm_min, int tm_hour, int tm_mday,
                int tm_mon, int tm_year, int tm_wday, int tm_yday,
                int tm_isdst, int msec, String tzname) {
            this.tm_sec = tm_sec;
            this.tm_min = tm_min;
            this.tm_hour = tm_hour;
            this.tm_mday = tm_mday;
            this.tm_mon = tm_mon;
            this.tm_year = tm_year;
            this.tm_wday = tm_wday;
            this.tm_yday = tm_yday;
            this.tm_isdst = tm_isdst;
            this.msec = msec;
            this.tzname = tzname;
        }

        public static Datetime fromLong(long millisec) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(millisec);
            return new Datetime(
                    cal.get(Calendar.SECOND),
                    cal.get(Calendar.MINUTE),
                    cal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.DAY_OF_MONTH),
                    cal.get(Calendar.MONTH),
                    cal.get(Calendar.YEAR) - 1900,
                    cal.get(Calendar.DAY_OF_WEEK),
                    cal.get(Calendar.DAY_OF_YEAR),
                    (cal.get(Calendar.DST_OFFSET) == 0) ? 0 : 1,
                    cal.get(Calendar.MILLISECOND),
                    cal.getTimeZone().getID()
                    );
        }

        @Override public String toString() {
            return String.format("%4d-%02d-%02dT%02d:%02d:%02d.%03d %s",
                    tm_year + 1900, tm_mon + 1, tm_mday, tm_hour, tm_min,
                    tm_sec, msec, tzname);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(capacity());
            bb.putInt(tm_sec).putInt(tm_min).putInt(tm_hour).putInt(tm_mday)
                .putInt(tm_mon).putInt(tm_year).putInt(tm_wday)
                .putInt(tm_yday).putInt(tm_isdst).putInt(msec);
            if (tzname != null)
                bb.put(tzname.getBytes());
            int curr = bb.position();
            byte[] zero = new byte[MAXT_TZ_LEN];
            bb.put(zero, 0, capacity() - curr);
            return bb.array();
        }

        @Override public int capacity() {
            return 4 * 10 + MAXT_TZ_LEN;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {

            if (bytes.length < capacity())
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            tm_sec = bb.getInt();
            tm_min = bb.getInt();
            tm_hour = bb.getInt();
            tm_mday = bb.getInt();
            tm_mon = bb.getInt();
            tm_year = bb.getInt();
            tm_wday = bb.getInt();
            tm_yday = bb.getInt();
            tm_isdst = bb.getInt();

            msec = bb.getInt();
            int head = bb.position(), tail = head;

            for (; tail < bytes.length && bytes[tail] != 0; ++tail)
                ;

            tzname = new String(bytes, head, tail - head);
            return true;

        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Datetime",
                    "boolean");
        }

        @Override public long toLong() {
            TimeZone tz = TimeZone.getTimeZone(tzname);
            Calendar cal = Calendar.getInstance(tz);
            cal.set(tm_year + 1900, tm_mon, tm_mday,
                    tm_hour, tm_min, tm_sec);
            long ms = cal.getTimeInMillis() / 1000L * 1000L + msec;
            return ms;
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Datetime",
                    "double");
        }
    }

    /* DatetimeUs - datetimeus */
    public static class DatetimeUs implements ByteArray {
        int tm_sec;
        int tm_min;
        int tm_hour;
        int tm_mday;
        int tm_mon;
        int tm_year;
        int tm_wday;
        int tm_yday;
        int tm_isdst;

        int usec;
        String tzname;

        public final static int MAXT_TZ_LEN = 36;

        public DatetimeUs() {
        /* default constructor */
        }

        public DatetimeUs(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        public DatetimeUs(int tm_sec, int tm_min, int tm_hour, int tm_mday,
                int tm_mon, int tm_year, int tm_wday, int tm_yday,
                int tm_isdst, int usec, String tzname) {
            this.tm_sec = tm_sec;
            this.tm_min = tm_min;
            this.tm_hour = tm_hour;
            this.tm_mday = tm_mday;
            this.tm_mon = tm_mon;
            this.tm_year = tm_year;
            this.tm_wday = tm_wday;
            this.tm_yday = tm_yday;
            this.tm_isdst = tm_isdst;
            this.usec = usec;
            this.tzname = tzname;
        }

        public static DatetimeUs fromLong(long microsec) {
            Calendar cal = Calendar.getInstance();
            /* sorry. lazy */
            cal.setTimeInMillis(microsec / 1000L);
            return new DatetimeUs(
                    cal.get(Calendar.SECOND),
                    cal.get(Calendar.MINUTE),
                    cal.get(Calendar.HOUR_OF_DAY),
                    cal.get(Calendar.DAY_OF_MONTH),
                    cal.get(Calendar.MONTH),
                    cal.get(Calendar.YEAR) - 1900,
                    cal.get(Calendar.DAY_OF_WEEK),
                    cal.get(Calendar.DAY_OF_YEAR),
                    (cal.get(Calendar.DST_OFFSET) == 0) ? 0 : 1,
                    (int)(microsec % 1000000L),
                    cal.getTimeZone().getID()
                    );
        }

        @Override public String toString() {
            return String.format("%4d-%02d-%02dT%02d:%02d:%02d.%06d %s",
                    tm_year + 1900, tm_mon + 1, tm_mday, tm_hour, tm_min,
                    tm_sec, usec, tzname);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(capacity());
            bb.putInt(tm_sec).putInt(tm_min).putInt(tm_hour).putInt(tm_mday)
                .putInt(tm_mon).putInt(tm_year).putInt(tm_wday)
                .putInt(tm_yday).putInt(tm_isdst).putInt(usec);
            if (tzname != null)
                bb.put(tzname.getBytes());
            int curr = bb.position();
            byte[] zero = new byte[MAXT_TZ_LEN];
            bb.put(zero, 0, capacity() - curr);
            return bb.array();
        }

        @Override public int capacity() {
            return 4 * 10 + MAXT_TZ_LEN;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {

            if (bytes.length < capacity())
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            tm_sec = bb.getInt();
            tm_min = bb.getInt();
            tm_hour = bb.getInt();
            tm_mday = bb.getInt();
            tm_mon = bb.getInt();
            tm_year = bb.getInt();
            tm_wday = bb.getInt();
            tm_yday = bb.getInt();
            tm_isdst = bb.getInt();

            usec = bb.getInt();
            int head = bb.position(), tail = head;

            for (; tail < bytes.length && bytes[tail] != 0; ++tail)
                ;

            tzname = new String(bytes, head, tail - head);
            return true;

        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Datetimeus",
                    "boolean");
        }

        @Override public long toLong() {
            TimeZone tz = TimeZone.getTimeZone(tzname);
            Calendar cal = Calendar.getInstance(tz);
            cal.set(tm_year + 1900, tm_mon, tm_mday,
                    tm_hour, tm_min, tm_sec);
            long us = cal.getTimeInMillis() / 1000L * 1000000L + usec;
            return us;
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Datetimeus",
                    "double");
        }
    }

    /* IntervalYearMonth - intervalym */
    public static class IntervalYearMonth implements ByteArray {
        int sign;
        int years;
        int months;

        public IntervalYearMonth() {
        /* default constructor */
        }

        public IntervalYearMonth(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        public IntervalYearMonth(int sign, int years, int months) {
            this.sign = sign;
            this.years = years;
            this.months = months;
        }

        @Override public String toString() {
            return String.format("%d-%d", years * sign, months);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(capacity());
            return bb.putInt(sign).putInt(years).putInt(months).array();
        }

        @Override public int capacity() {
            return 12;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            if (bytes.length < 12)
                return false;
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            sign = bb.getInt();
            years = bb.getInt();
            months = bb.getInt();
            return true;
        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Intervalym",
                    "boolean");
        }

        @Override public long toLong() {
            throw new UnsupportedConversionException("Comdb2 Intervalym",
                    "long");
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Intervalym",
                    "double");
        }
    }

    /* IntervalDaySecond - intervalds */
    public static class IntervalDaySecond implements ByteArray {
        int sign;
        int days;
        int hours;
        int mins;
        int sec;
        int msec;

        public IntervalDaySecond() {
        /* default constructor */
        }

        public IntervalDaySecond(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        public IntervalDaySecond(int sign, int days, int hours, int mins,
                int sec, int msec) {
            this.sign = sign;
            this.days = days;
            this.hours = hours;
            this.mins = mins;
            this.sec = sec;
            this.msec = msec;
        }

        @Override public String toString() {
            return String.format("%d %d:%d:%d.%03d", days * sign, hours, mins,
                    sec, msec);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(capacity());
            return bb.putInt(sign).putInt(days).putInt(hours).putInt(mins)
                .putInt(sec).putInt(msec).array();
        }

        @Override public int capacity() {
            return 4 * 6;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            if (bytes.length < 24)
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            sign = bb.getInt();
            days = bb.getInt();
            hours = bb.getInt();
            mins = bb.getInt();
            sec = bb.getInt();
            msec = bb.getInt();
            return true;
        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "boolean");
        }

        @Override public long toLong() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "long");
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "double");
        }
    }

    /* IntervalDaySecondUs - intervaldsus */
    public static class IntervalDaySecondUs implements ByteArray {
        int sign;
        int days;
        int hours;
        int mins;
        int sec;
        int usec;

        public IntervalDaySecondUs() {
        /* default constructor */
        }

        public IntervalDaySecondUs(byte[] bytes) {
            reconstructFromBytes(bytes);
        }

        public IntervalDaySecondUs(int sign, int days, int hours, int mins,
                int sec, int usec) {
            this.sign = sign;
            this.days = days;
            this.hours = hours;
            this.mins = mins;
            this.sec = sec;
            this.usec = usec;
        }

        @Override public String toString() {
            return String.format("%d %d:%d:%d.%06d", days * sign, hours, mins,
                    sec, usec);
        }

        @Override public byte[] toBytes() {
            ByteBuffer bb = ByteBuffer.allocate(capacity());
            return bb.putInt(sign).putInt(days).putInt(hours).putInt(mins)
                .putInt(sec).putInt(usec).array();
        }

        @Override public int capacity() {
            return 4 * 6;
        }

        @Override public boolean reconstructFromBytes(byte[] bytes) {
            if (bytes.length < 24)
                return false;

            ByteBuffer bb = ByteBuffer.wrap(bytes);
            sign = bb.getInt();
            days = bb.getInt();
            hours = bb.getInt();
            mins = bb.getInt();
            sec = bb.getInt();
            usec = bb.getInt();
            return true;
        }

        @Override public boolean toBool() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "boolean");
        }

        @Override public long toLong() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "long");
        }

        @Override public double toDouble() {
            throw new UnsupportedConversionException("Comdb2 Intervalds",
                    "double");
        }
    }
}
/* vim: set sw=4 ts=4 et: */
