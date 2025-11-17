#!/usr/bin/env /proj/sot/ska3/flight/bin/python

"""
Glimmon database interface using SQLAlchemy ORM with additional functions.
"""

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

GLIMMON = "/data/mta/Script/MSID_limit/glimmondb.sqlite3"

class Base(DeclarativeBase):
    pass
class Limits(Base):
    """
    SQLAlchemy ORM of Glimmon databse. Subject to schema decisions of OPS
    
    https://occweb.cfa.harvard.edu/twiki/bin/view/ChandraCode/G_LIMMONSQLite3Database
    """
    __tablename__ = "limits"
    
    id: Mapped[int] = mapped_column(primary_key=True, comment="Unique identifier used by the database.")
    msid: Mapped[str] = mapped_column(comment="MSID Name.")
    setkey: Mapped[int] = mapped_column(comment="Set number allocated to each MSID in G_LIMMON.dec.")
    datesec: Mapped[float] = mapped_column(comment="Date in seconds from 1997:365:23:58:56.816 (epoch for Ska environment).")
    date: Mapped[str] = mapped_column(comment="Date (YYYY-MM-DD hh:mm:ss.ss) when each MSID/set pair (row) was added/modified/deleted in G_LIMMON.dec.")
    modversion: Mapped[int] = mapped_column(comment="Version each MSID/set pair was added under, corresponds to date and datesec columns.")
    mlmenable: Mapped[bool] = mapped_column(comment="0 or 1 indicating whether or not the MSID/set pair is active from that point forward. 0 means the pair is deactivated. An MSID can be deactivated explicitly in G_LIMMON using the 'MLMENABLE 0' syntax or implicitly by deleting the msid/set pair from G_LIMMON.")
    mlmtol: Mapped[int] = mapped_column(comment="Limit glitch tolerance (e.g. a value of two requires the MSID to be outside of its limits for two updates before being flagged as a violation).")
    default_set: Mapped[int] = mapped_column(comment="The default limit set to be used for each MSID, can only be 1+ when additional sets are defined.")
    mlimsw: Mapped[str] = mapped_column(comment="MSID used to switch limit sets.")
    caution_high: Mapped[float] = mapped_column(comment="Caution (yellow) upper limit.")
    caution_low: Mapped[float] = mapped_column(comment="Caution (yellow) lower limit.")
    warning_high: Mapped[float] = mapped_column(comment="Warning (red) upper limit")
    warning_low: Mapped[float] = mapped_column(comment="Warning (red) lower limit")
    switchstate: Mapped[str] = mapped_column(comment="State for MSID defined in mlimsw that corresponds to the limit set defined in the same row.")
        
    def to_dict(self):
        """Maps table columns to value as a python dictionary"""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self) -> str:
         return f"Limits(id={self.id!r}, msid={self.msid!r}, caution=[{self.caution_low!r},{self.caution_high!r}], warning=[{self.warning_low!r},{self.warning_high!r}])"