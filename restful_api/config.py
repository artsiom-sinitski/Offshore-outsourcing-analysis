
"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/08/2020
"""

import os
# from credentials import db_credentials

class Development(object):
    """
    Development environment configuration
    """
    DEBUG = True
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')
    # app.config['SQLALCHEMY_DATABASE_URI'] = \
    #     'postgresql://%(user)s:%(password)s@%(host)s:%(port)s/%(db_name)s' % db_credentials


class Production(object):
    """
    Production environment configurations
    """
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')


app_config = {
    'development': Development,
    'production': Production,
}