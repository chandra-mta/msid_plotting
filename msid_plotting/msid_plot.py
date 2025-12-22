#!/usr/bin/env /proj/sot/ska3/flight/bin/python

"""
Plotting classes for multivariate MSID plots using bokeh for interactivity.

:NOTE: Ska3 Cheta contains MSID plotting classes for at-runtime interactive plots,
    however they are not tailored for scripts, multivariate, or specifics of web services.
    This may change with future developments, in which case consider conforming to Ska3 standard.
"""

from . import msid_limit

#: Ska3
import kadi.events
from cxotime import CxoTime
import maude

#: Calculation
from datetime import datetime, timedelta, time
import numexpr as ne
import numpy as np
import os

#: Formatting
from typing import Any, cast, List
from pprint import pformat
from jinja2 import Environment, PackageLoader, FileSystemLoader, ChoiceLoader

#: Plotting
from bokeh.plotting import figure  #: 1.89usec
from bokeh.layouts import gridplot  #: 1.83 usec
from bokeh.models import DatetimeTickFormatter  #: 1.7 usec
from bokeh.resources import CDN
from bokeh.embed import file_html

_T1998 = 883612736.816  #: Difference between Chandra and Epoch Time

@np.vectorize
def _vecdatetime(x):
    if isinstance(x, (int, float)):
        return datetime.fromtimestamp(round(x))

def _resize(alpha, beta):
        if alpha.size < beta.size:
            return beta[:alpha.size]
        elif alpha.size > beta.size:
            for i in range(beta.size, alpha.size):
                beta = np.append(beta, beta[-1])
            return beta
        else:
            return beta

class JinjaTemplateEnv(object):
    """
    Singleton class for Jinja Enviroment tempaltes with additional template handling methods
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            #: If no TemplateEnv instance exists, create a new one
            cls._instance = super().__new__(cls)
        return cls._instance  #: Return the existing instance

    def __init__(self):
        if not hasattr(self, "_initialized"):  #: Prevent re-initialization
            self._initialized = True
            self.env = Environment(loader=ChoiceLoader([
                                     PackageLoader("msid_plotting", "template"),
                                     ]))
    def _add_loader(self, loader) -> None:
        """
        Allows for adding additional Jinja Loaders, but common usage is a file path for FileSystemLoader
        """
        self.env.loader.loaders.append(loader) # type: ignore
    
    def add_template_directory(self, filepath : str, **kwargs) -> None:
        """
        Add another template directory to the jinja templater.
        """
        if os.path.isdir(filepath):
            _loader = FileSystemLoader(filepath, **kwargs)
            self._add_loader(_loader)
        else:
            if os.path.isfile(filepath):
                raise FileNotFoundError("Specified path is not a directory.")
            else:
                raise FileNotFoundError(f"Could not find filepath: {filepath}")

JINJA_TEMPLATE_ENV = JinjaTemplateEnv()

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
        if not hasattr(self, "_initialized"):  #: Prevent re-initialization
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
        _track_start = CxoTime(
            datetime.combine(
                _dt_start.date(),
                time(hour=int(self.comm.bot[:2]), minute=int(self.comm.bot[2:])),
            )
        )

        if _track_start < _start:  #: Time after midnight
            _track_start += timedelta(days=1)

        #: Stop
        _dt_stop = cast(datetime, _stop.datetime)
        _track_stop = CxoTime(
            datetime.combine(
                _dt_stop.date(),
                time(hour=int(self.comm.eot[:2]), minute=int(self.comm.eot[2:])),
            )
        )

        if _track_stop > _stop:  #: Time before midnight
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
        return " ".join(
            [
                f"<{self.__class__.__name__}:",
                f"time={self.current_time.date},",
                f"track_start={self.track_start.date}>",
            ]
        )

    def __str__(self):
        return pformat(self.__dict__)

class MSIDPlot(object):
    """
    Class for plotting parameters of Multivariate MSID interactive plot

    :NOTE: Maude stores MSID's in uppercase
    """

    #: Type Hint
    fetch_result: dict[str, Any]

    def __init__(self, msids, start, stop):
        if isinstance(msids, list):
            self.msids = [_.upper() for _ in msids]
        elif isinstance(msids, str):
            self.msids = [msids.upper()]
        else:
            raise Exception("MSIDPlot input must be an MSID or a list of MSIDs.")

        self.start = start
        self.stop = stop
    
    def _query_maude(self, msids, **kwargs) -> dict:

        fetch_result = maude.get_msids(
            msids=msids, start=self.start, stop=self.stop, **kwargs
        )
        return fetch_result
    
    def fetch_data(self, bin_size = 500, forcerun = False) -> None:
        """
        Fetch the MSID Plot telemetry from the maude server and assign the raw fetch result
        """
        if not hasattr(self, 'fetch_result') or forcerun:
            self.fetch_result = self._query_maude(msids=self.msids)

        values = {}
        datetimes = {}
        for result in self.fetch_result['data']:
            #: Downsample Step
            if bin_size is not None:
                _slice_step = result['n_values'] // bin_size
                if _slice_step == 0:
                    _slice_step = 1
            else:
                _slice_step = 1
            
            values[result['msid']] = result['values'][::_slice_step]
            _cxotimes = result['times'][::_slice_step]
            #: Fast numerical conversions to format cxosecs into Bokeh-plottable datetimes
            datetimes[result['msid']] = _vecdatetime(ne.evaluate("_cxotimes + _T1998"))
        
        self.values = values
        self.datetimes = datetimes

    def fetch_limit(self) -> None:
        """
        Use the limit API to fetch and set limits for the current msid set.
        """
        _limits = msid_limit.query_msid_limits(self.msids)
        self.limits = _limits


    def _generate_frames(self) -> List[Any]:
        frames = []

        for msid in self.msids:
            p = figure(title="Test", y_axis_label=msid,x_axis_label="Date", x_axis_type = 'datetime')
            p.scatter(x=self.datetimes[msid], y=self.values[msid])

            p.xaxis.formatter = DatetimeTickFormatter(
                minutes="%Y:%j:%H:%M",
                hours="%Y:%j:%H",
                hourmin="%Y:%j:%H:%M",
                days="%Y:%j",
            )

            frames.append([p])
        return frames
        
    def generate_plot_html(self, template_name = None) -> str:
        """
        Generate plot frames and write the contents into a python jinja template.
        """
        frames = self._generate_frames()
        plot = gridplot(frames)
        if template_name is None: 
            template = JINJA_TEMPLATE_ENV.env.get_template("plot.jinja")
        else:
            template = JINJA_TEMPLATE_ENV.env.get_template(template_name)

        html = file_html(plot, CDN, template=template)

        return html