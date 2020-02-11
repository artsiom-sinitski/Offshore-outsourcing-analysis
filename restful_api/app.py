"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/08/2020
"""
from flask import Flask, render_template, request,\
                  redirect, url_for, jsonify
from event_model import GdeltEventModel, GdeltEventSchema, rdb, ma
from mention_model import GdeltMentionModel, GdeltMentionSchema
from config import app_config
import connexion

# # Sqlalchemy - flask wrapper for Sqlalchemy. SqlAlchemy is an Object Relational Mapper (ORM).
# # An ORM is a database sql abstraction to carry out sql operations on relational database.
# from flask_sqlalchemy import SQLAlchemy
# # Marshmallow is a library for converting complex datatypes to and from native Python datatypes.
# # It's used for deserialization (converting data to application objects) and
# # serialization (converting application objects to simple types).
# from flask_marshmallow import Marshmallow

# rdb = SQLAlchemy()
# ma = Marshmallow()

# Create the application instance using connexion
# app = connexion.FlaskApp(__name__, specification_dir='./')
# app.add_api('swagger.yaml')
# print(" -------- Type: ", type(app.app))
# exit()

app = Flask(__name__, template_folder="templates")
app.config.from_object(app_config['development'])
                                                         
rdb.init_app(app)
ma.init_app(app)

event_schema = GdeltEventSchema()
event_list_schema = GdeltEventSchema(many=True)

mention_schema = GdeltMentionSchema()
mention_list_schema = GdeltMentionSchema(many=True)


@app.route('/', methods=['GET'])
def home():
    return '''<h1> Data Warehouse Solution (DWS)</h1>
              <p> A prototype API for data retrieval from DWS.</p>
           '''
    # return render_template('home.html')    # need to create a template for "home.html"

#================== GDELT Events API mappings section ==================================
@app.route('/dws_api/event_by_id_req/<id>', methods=['GET'])
def event_by_id_request(id):
    event = GdeltEventModel.get_event_by_id(id)
    return event_schema.dump(event)

#TODO: need to make it work
@app.route('/dws_api/event_list_by_date_req/<date>', methods=['GET'])
def events_by_date_request(date):
    event_list = GdeltEventModel.get_events_by_date(date)
    return jsonify({ "event_list": event_list_schema.dump(event_list) })


#================== GDELT Mentions API mappings section ==================================
@app.route('/dws_api/mention_by_id_req/<id>', methods=['GET'])
def mention_by_id_request(id):
    mention = GdeltMentionModel.get_mention_by_id(id)
    return mention_schema.dump(mention)

#================== GDELT Gkg API mappings section =======================================
#TODO:


# If we're running in stand alone mode, run the application
if __name__ == '__main__':
    # app.config['DEBUG'] = True
    app.run(host='0.0.0.0', port=5000)
