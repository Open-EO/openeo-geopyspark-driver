from openeogeotrellis import app
from flask import Flask, request, url_for, redirect

@app.route('/v0.1')
def index():
    return 'OpenEO GeoPyspark backend. ' + url_for('timeseries')

@app.route('/v0.1/timeseries')
def timeseries():
    return 'OpenEO GeoPyspark backend. ' + url_for('point')


@app.route('/v0.1/timeseries/point', methods=['GET', 'POST'])
def point():
    if request.method == 'POST':
        x = request.args.get('x', '')
        y = request.args.get('y', '')
        srs = request.args.get('srs', '')
        startdate = request.args.get('startdate', '')
        enddate = request.args.get('enddate', '')
        process_graph = request.json
        return process_graph
    else:
        return 'Usage: Query point timeseries.'