from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://localhost/solar_plants'  # Change this for Heroku
app.config['SQLALCHEMY_DATABASE_URI'] ='postgres://u6vursk1m6enlu:p375f4bba0d873a9a2c55607f3f38890300cbe538fac836de988e4de432d4e375@c1i13pt05ja4ag.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/de71fjudo078hq'

db = SQLAlchemy(app)

class SolarPlant(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    size = db.Column(db.Float, nullable=False)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    angle = db.Column(db.Float, nullable=False)
    max_power = db.Column(db.Float, nullable=False)
    owner_name = db.Column(db.String(100), nullable=False)
    owner_account = db.Column(db.String(50), nullable=False)
    grid_substation = db.Column(db.String(100), nullable=False)
    connected_feeder = db.Column(db.String(100), nullable=False)

@app.route('/')
def index():
    plants = SolarPlant.query.all()
    return render_template('index.html', plants=plants)

@app.route('/add', methods=['GET', 'POST'])
def add_plant():
    if request.method == 'POST':
        new_plant = SolarPlant(
            name=request.form['name'],
            size=float(request.form['size']),
            latitude=float(request.form['latitude']),
            longitude=float(request.form['longitude']),
            angle=float(request.form['angle']),
            max_power=float(request.form['max_power']),
            owner_name=request.form['owner_name'],
            owner_account=request.form['owner_account'],
            grid_substation=request.form['grid_substation'],
            connected_feeder=request.form['connected_feeder']
        )
        db.session.add(new_plant)
        db.session.commit()
        return redirect(url_for('index'))
    return render_template('add_plant.html')

if __name__ == '__main__':
    app.run(debug=True)