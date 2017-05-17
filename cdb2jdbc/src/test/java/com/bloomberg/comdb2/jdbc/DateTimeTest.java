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

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import com.bloomberg.comdb2.jdbc.Cdb2Types.Datetime;

/*
 * @author Junmin Liu
 */
public class DateTimeTest {

    @Test public void fromLong(){

        Date d = new Date();		
        Datetime dt = Datetime.fromLong(d.getTime());
        assertNotNull(dt.tzname);
        assertTrue(!dt.tzname.isEmpty());
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(d.getTime());
        assertTrue(dt.tm_hour == cal.get(Calendar.HOUR_OF_DAY));

    }

    @Test public void toLongWithTimezone(){
        Calendar cal = Calendar.getInstance();
        Cdb2Types.Datetime dt = new Datetime(cal.get(Calendar.SECOND), cal.get(Calendar.MINUTE), cal.get(Calendar.HOUR_OF_DAY),
                cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.MONTH), cal.get(Calendar.YEAR) - 1900, cal.get(Calendar.DAY_OF_WEEK),
                cal.get(Calendar.DAY_OF_YEAR), 0, cal.get(Calendar.MILLISECOND), cal.getTimeZone().getID());

        assertTrue(dt.toLong() == cal.getTimeInMillis());

    }

    @Test public void toByte(){
        Date d = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(d.getTime());
        Datetime dt = Datetime.fromLong(d.getTime());

        byte[] bytes = dt.toBytes();

        int sec = (bytes[0]<<24)&0xff000000|
            (bytes[1]<<16)&0x00ff0000|
            (bytes[2]<< 8)&0x0000ff00|
            (bytes[3]<< 0)&0x000000ff;

        assertEquals(cal.get(Calendar.SECOND),  sec);

        int min = (bytes[4]<<24)&0xff000000|
            (bytes[5]<<16)&0x00ff0000|
            (bytes[6]<< 8)&0x0000ff00|
            (bytes[7]<< 0)&0x000000ff;

        assertEquals(cal.get(Calendar.MINUTE),  min);
    }
}
/* vim: set sw=4 ts=4 et: */
