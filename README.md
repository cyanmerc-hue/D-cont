# D-cont

A simple Flask web application with user login and registration.

## Features
- User registration
- User login/logout
- SQLite database for user storage

## How to Run
1. Make sure you have Python 3.14+ installed.
2. Install dependencies (Flask):
   ```
pip install flask
   ```
3. Run the app:
   ```
python app.py
   ```
4. Open your browser and go to http://127.0.0.1:5000/

## Folder Structure
- `app.py` - Main Flask application
- `templates/` - HTML templates
- `static/` - Static files (CSS, JS, images)

## Note
- Default secret key and password storage are for demo only. For production, use hashed passwords and a secure secret key.
