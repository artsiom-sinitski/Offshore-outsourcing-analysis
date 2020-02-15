"""
Author: Artsiom Sinitski
Email:  artsiom.vs@gmail.com
Date:   02/10/2020
"""

from flask import Flask, render_template


app = Flask(__name__, template_folder="templates")

@app.route('/')
def main():
    return render_template("dws_demo.html")


if __name__ == "__main__":
    # app.config[DEBUG] = True 
    app.run(host='0.0.0.0', port=80)


