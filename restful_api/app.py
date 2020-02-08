"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/08/2020
"""
from flask import Flask, request, redirect, url_for, jsonify
from event_model import GdeltEventModel, GdeltEventSchema, rdb, ma
from credentials import db_credentials


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] =\
    'postgresql://%(user)s:%(password)s@%(host)s:%(port)s/%(db_name)s' % db_credentials
                                                         

rdb.init_app(app)
ma.init_app(app)

event_schema = GdeltEventSchema()
# events_list_schema = GdeltEventModel(many=True)


@app.route('/')
def home():
    return '''<h1> Data Warehouse Solution (DWS)</h1>
              <p> A prototype API for data retrieval from DWS.</p>
           '''


@app.route('/dws_api/gdelt_event_by_id_req/<id>')
def event_by_id_request(id):
    gdelt_event = GdeltEventModel.get_event_by_id(id)
    return event_schema.dump(gdelt_event)


# If we're running in stand alone mode, run the application
if __name__ == '__main__':
    app.config['DEBUG'] = True
    app.run(host='0.0.0.0', port=5000)
