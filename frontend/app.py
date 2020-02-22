"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/10/2020
"""

from flask import Flask, render_template,\
                  request, url_for, redirect


app = Flask(__name__, template_folder="templates")

@app.route('/')
def index():
    return render_template("index.html")


@app.route('/webapi', methods=['GET'])
def webapi():
    return render_template("web_api.html")


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)


