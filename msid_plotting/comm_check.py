#!/usr/bin/env / proj/sot/ska3/flight/bin/python

"""
Convenience functions for checking DSN Comm information using Kadi
"""

#: Ska3
import kadi.events
from cxotime import CxoTime

#: Time
from datetime import datetime, timedelta, time

def comm_check(checktime = None):
    if checktime is None:
        checktime = CxoTime() #: Checking the current time

    dsn_query = kadi.events.dsn_comms.filter(start=checktime)
    comm = dsn_query[0] #: Upcomming or current comm
    backstop_query = kadi.events.dsn_comms.filter(ifot_id = comm.ifot_id - 1)
    previous_comm = backstop_query[0]
    
    return {
        'comm': comm,
        'previous_comm': previous_comm,
        'dsn_query': dsn_query
    }

def translate(dsn_comm):
        """
        Translate the Kadi Event DSN Comm query result into CxoTime
        """
        support_start = CxoTime(dsn_comm.start)
        support_stop = CxoTime(dsn_comm.stop)

        #: Start
        dt_start = support_start.datetime
        track_start = CxoTime(
            datetime.combine(
                dt_start.date(), # type: ignore
                time(hour=int(dsn_comm.bot[:2]), minute=int(dsn_comm.bot[2:])),
            )
        )

        if track_start < support_start:  #: Time after midnight
            track_start += timedelta(days=1)

        #: Stop
        dt_stop = support_stop.datetime
        track_stop = CxoTime(
            datetime.combine(
                dt_stop.date(), # type: ignore
                time(hour=int(dsn_comm.eot[:2]), minute=int(dsn_comm.eot[2:])),
            )
        )

        if track_stop > support_stop:  #: Time before midnight
            track_stop -= timedelta(days=1)

        return {
            'support_start': support_start,
            'support_stop': support_stop,
            'track_start': track_start,
            'track_stop': track_stop
        }

def in_state(
        support_start,
        support_stop,
        track_start,
        track_stop,
        checktime = None
    ):
        """
        Compute if the current or provided Cxotime is in the provided support or track times
        """
        if checktime is None:
            checktime = CxoTime() #: Checking the current time
        if support_start < checktime < support_stop:
            in_support = True
        else:
            in_support = False
        if track_start < checktime < track_stop:
            in_track = True
        else:
            in_track = False
        
        return {
            'in_support': in_support,
            'in_track': in_track
        }