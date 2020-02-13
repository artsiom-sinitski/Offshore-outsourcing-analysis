"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/09/2020
"""
# # Sqlalchemy - flask wrapper for Sqlalchemy. SqlAlchemy is an Object Relational Mapper (ORM).
# # An ORM is a database sql abstraction to carry out sql operations on relational database.
from flask_sqlalchemy import SQLAlchemy
# # Marshmallow is a library for converting complex datatypes to and from native Python datatypes.
# # It's used for deserialization (converting data to application objects) and
# # serialization (converting application objects to simple types).
from flask_marshmallow import Marshmallow
from flask import request
# from app import rdb, ma

rdb = SQLAlchemy()
ma = Marshmallow()

class GdeltMentionModel(rdb.Model):
    """
    GDELT mention model class definition.
    Model determines the logical structures of a database and defines how
    records can be manipulated or retrieved in the database.
    """
    __tablename__ = 'gdelt_mentions'

    GlobalEventId = rdb.Column(rdb.String(128), primary_key=True)
    MentionTimeDate = rdb.Column(rdb.Date, nullable=False)
    MentionType = rdb.Column(rdb.String(128))
    MentionSourceName = rdb.Column(rdb.String(128))
    MentionIdentifier = rdb.Column(rdb.String(255))


    def __init__(self, data):
        """
        Class constructor
        """
        self.GlobalEventId = data.get('GlobalEventId')
        self.MentionTimeDate = data.get('MentionTimeDate')
        self.MentionType = data.get('MentionType')
        self.MentionSourceName = data.get('MentionSourceName')
        self.MentionIdentifier = data.get('MentionIdentifier')
 

    @staticmethod
    def get_mention_by_id(id):
        return GdeltMentionModel.query.get(id)


    @staticmethod
    def get_all_mentions():
        return GdeltMentionModel.query.all()

# ================================================================

class GdeltMentionSchema(ma.TableSchema):
    class Meta:
        table = GdeltMentionModel.__table__
