#!/usr/bin/env /proj/sot/ska3/flight/bin/python

"""
Plotting classes for multivariate MSID plots using bokeh for interactivity.

:NOTE: Ska3 Cheta contains MSID plotting classes for at-runtime interactive plots,
    however they are not tailored for scripts, multivariate, or specifics of web services.
    This may change with future developments, in which case consider conforming to Ska3 standard.
"""
import kadi.events
from cxotime import CxoTime
from datetime import datetime, timedelta, time
from pprint import pformat


class MSIDPlot(object):
    """
    Class for Plotting parameters of Multivariate MSID interactive plot
    """
    def __init__(self,msids):
        """
        Initialization
        """
        if isinstance(msids, list):
            self.msids = [_.upper() for _ in msids]
        elif isinstance(msids, str):
            self.msids = [msids.upper()]
        else:
            raise Exception("MSIDPlot input but be an MSID or a list of MSIDs.")
        

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
        _start = CxoTime(self.comm.start)
        _stop = CxoTime(self.comm.stop)
        #: Start
        _track_start = CxoTime(datetime.combine(_start.datetime.date(),
                                                time(hour = int(self.comm.bot[:2]),
                                                     minute = int(self.comm.bot[2:])
                                                    )
                                               )
                              )
        
        if _track_start < _start: #: Time after midnight
            _track_start += timedelta(days=1)
        #: Stop
        _track_stop = CxoTime(datetime.combine(_stop.datetime.date(),
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
