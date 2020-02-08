"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/08/2020
"""
# Sqlalchemy - flask wrapper for Sqlalchemy. SqlAlchemy is an Object Relational Mapper (ORM).
# An ORM is a database sql abstraction to carry out sql operations on relational database.
from flask_sqlalchemy import SQLAlchemy
# Marshmallow is a library for converting complex datatypes to and from native Python datatypes.
# It's used for deserialization (converting data to application objects) and
# serialization (converting application objects to simple types).
from flask_marshmallow import Marshmallow


rdb = SQLAlchemy()
ma = Marshmallow()

class GdeltEventModel(rdb.Model):
    """
    GDELT event model class definition
    """
    __tablename__ = 'gdelt_events'

    GlobalEventId = rdb.Column(rdb.String(128), primary_key=True)
    SqlDate = rdb.Column(rdb.Date, nullable=False)
    Actor1Name = rdb.Column(rdb.String(128))
    Actor1CountryCode = rdb.Column(rdb.String(128))
    Actor2Name = rdb.Column(rdb.String(128))
    Actor2CountryCode = rdb.Column(rdb.String(128))
    EventCode = rdb.Column(rdb.String(128))
    AvgTone = rdb.Column(rdb.Float)


    def __init__(self, data):
        """
        Class constructor
        """
        self.GlobalEventId = data.get('GlobalEventId')
        self.SqlDate = data.get('SqlDate')
        self.Actor1Name = data.get('Actor1Name')
        self.Actor1CountryCode = data.get('Actor1CountryCode')
        self.Actor2Name = data.get('Actor2Name')
        self.Actor2CountryCode = data.get('Actor2CountryCode')
        self.EventCode = data.get('EventCode')
        self.AvgTone = data.get('AvgTone')


    @staticmethod
    def get_event_by_id(id):
        return GdeltEventModel.query.get(id)
    

    @staticmethod
    def get_all_events():
        return GdeltEventModel.query.all()

# ================================================================

class GdeltEventSchema(ma.TableSchema):
    class Meta:
        table = GdeltEventModel.__table__

