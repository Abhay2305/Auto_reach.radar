from flask import Flask, send_file, request, redirect, render_template
import csv
from datetime import datetime
import os

app = Flask(__name__)

TRACKING_FILE = 'tracking.csv'
TRANSPARENT_IMAGE = 'transparent.png'

@app.route('/')
def home():
    return "<h2>Welcome to the Email Tracking Server</h2><p>Visit <a href='/dashboard'>/dashboard</a> to see tracking logs.</p>"

@app.route('/pixel')
def pixel():
    tracking_id = request.args.get('id')
    email = request.args.get('email')
    log_event(email, tracking_id, 'OPEN')
    return send_file(TRANSPARENT_IMAGE, mimetype='image/png')

@app.route('/redirect')
def redirect_link():
    tracking_id = request.args.get('id')
    email = request.args.get('email')
    target_url = request.args.get('url', 'https://www.google.com')
    log_event(email, tracking_id, 'CLICK')
    return redirect(target_url)

@app.route('/dashboard')
def dashboard():
    rows = []
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if all(k in row and row[k] for k in ['email', 'tracking_id', 'event', 'time']):
                    rows.append(row)
    return render_template('dashboard.html', logs=rows)

def log_event(email, tracking_id, event_type):
    now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    file_exists = os.path.isfile(TRACKING_FILE)
    with open(TRACKING_FILE, 'a', newline='') as csvfile:
        fieldnames = ['email', 'tracking_id', 'event', 'time']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({'email': email, 'tracking_id': tracking_id, 'event': event_type, 'time': now})

if __name__ == '__main__':
    app.run(debug=True)
