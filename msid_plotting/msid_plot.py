#!/usr/bin/env /proj/sot/ska3/flight/bin/python

"""
Plotting classes for multivariate MSID plots using bokeh for interactivity.

:NOTE: Ska3 Cheta contains MSID plotting classes for at-runtime interactive plots,
    however they are not tailored for scripts, multivariate, or specifics of web services.
    This may change with future developments, in which case consider conforming to Ska3 standard.
"""
import msid_limit

#: Ska3
import kadi.events
from cxotime import CxoTime
import maude
#: Calculation
from datetime import datetime, timedelta, time
import numexpr as ne
import numpy as np
#: Formatting
from typing import Any, cast
from pprint import pformat

_T1998 = 883612736.816 #: Difference between Chandra and Epoch Time


@np.vectorize
def _vecdatetime(x):
    if isinstance(x, (int,float)):
        return datetime.fromtimestamp(round(x))

class MSIDPlot(object):
    """
    Class for plotting parameters of Multivariate MSID interactive plot
    """
    #: Type Hint
    fetch_result : dict[str, Any]

    def __init__(self,msids, start, stop):
        if isinstance(msids, list):
            self.msids = [_.upper() for _ in msids]
        elif isinstance(msids, str):
            self.msids = [msids.upper()]
        else:
            raise Exception("MSIDPlot input but be an MSID or a list of MSIDs.")
        
        self.start = start
        self.stop = stop

    def fetch_maude(self) -> None:
        """
        Fetch the MSID telemetry from the maude server.
        """
        self.fetch_result = maude.get_msids(
            msids = self.msids,
            start = self.start,
            stop = self.stop
        )
    
    def fetch_limit(self) -> None:
        """
        Use the limit API to fetch and set limits for the current msid set.
        """
        _limits = msid_limit.fetch_msid_limits(self.msids)
        self.limits = _limits

    def _to_datetime(self, forcerun=False) -> None:
        """
        Fast numerical conversions to format cxosecs into Bokeh-plottable datetimes
        """
        if not hasattr(self, '_datetimes') or forcerun:
            _datetimes = {}
            for idx in range(len(self.msids)): #: fetch result indexed by 
                _msid = self.fetch_result['data'][idx]['msid']
                _time = self.fetch_result['data'][idx]['times'] #: cxosecs
                _datetimes[_msid] = _vecdatetime(ne.evaluate("_time + _T1998"))
            self._datetimes = _datetimes

class CommCheck(object):
    """
    Singleton class for kadi-fetched comm information and related methods.

    Only used to check if currently in comm
    """
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            #: If no CommCheck instance exists, create a new one
            cls._instance = super().__new__(cls)
        return cls._instance  #: Return the existing instance

    def __init__(self):
        if not hasattr(self, '_initialized'): #: Prevent re-initialization
            self._initialized = True
            self.check()
    
    def check(self):
        """
        Sets the in-comm check information. Runs once at initialization then can be run again at will
        """
        self.current_time = CxoTime()
        self.dsn_query = kadi.events.dsn_comms.filter(start=self.current_time)
        self.in_support = False
        self.in_track = False
        self.comm = self.dsn_query[0]
        #: Identify Track Time (Data exchange during track during)
        _start = cast(CxoTime, CxoTime(self.comm.start))
        _stop = cast(CxoTime, CxoTime(self.comm.stop))

        #: Start
        _dt_start = cast(datetime, _start.datetime)
        _track_start = CxoTime(datetime.combine(_dt_start.date(),
                                                time(hour = int(self.comm.bot[:2]),
                                                     minute = int(self.comm.bot[2:])
                                                    )
                                               )
                              )
        
        if _track_start < _start: #: Time after midnight
            _track_start += timedelta(days=1)
        
        #: Stop
        _dt_stop = cast(datetime, _stop.datetime)
        _track_stop = CxoTime(datetime.combine(_dt_stop.date(),
                                                time(hour = int(self.comm.eot[:2]),
                                                     minute = int(self.comm.eot[2:])
                                                    )
                                               )
                              )
        
        if _track_stop > _stop: #: Time before midnight
            _track_stop -= timedelta(days=1)
        
        self.support_start = _start
        self.support_stop = _stop
        self.track_start = _track_start
        self.track_stop = _track_stop
        
        if _start < self.current_time < _stop:
            self.in_support = True
        if _track_start < self.current_time < _track_stop:
            self.in_track = True
            
    def __repr__(self):
        return (' '.join([f'<{self.__class__.__name__}:',
                          f'time={self.current_time.date},',
                          f'track_start={self.track_start.date}>']))


    def __str__(self):
        return pformat(self.__dict__)