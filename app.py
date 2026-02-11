from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import data_manager
import config
import pandas as pd
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Needed for flash messages
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/config')
def config_page():
    configs = data_manager.get_column_configs()
    return render_template('config.html', configs=configs)

@app.route('/config/update', methods=['POST'])
def update_config():
    column_id = request.form.get('id')
    is_mandatory = request.form.get('is_mandatory') == 'on'
    data_type = request.form.get('data_type')
    
    if data_manager.update_column_config(column_id, is_mandatory, data_type):
        flash("Configuration updated successfully.", "success")
    else:
        flash("Failed to update configuration.", "error")
    return redirect(url_for('config_page'))

@app.route('/config/alias/add', methods=['POST'])
def add_alias():
    column_id = request.form.get('column_id')
    alias_name = request.form.get('alias_name')
    
    if alias_name and data_manager.add_alias(column_id, alias_name):
        flash("Alias added.", "success")
    else:
        flash("Failed to add alias.", "error")
    return redirect(url_for('config_page'))

@app.route('/config/alias/delete/<int:alias_id>')
def delete_alias(alias_id):
    if data_manager.delete_alias(alias_id):
        flash("Alias deleted.", "success")
    else:
        flash("Failed to delete alias.", "error")
    return redirect(url_for('config_page'))

@app.route('/')
def index():
    conn = data_manager.get_connection()
    data = []
    if conn:
        try:
            query = f"SELECT * FROM {config.TABLE_NAME}"
            df = pd.read_sql(query, conn)
            data = df.to_dict(orient='records')
        except Exception as e:
            flash(f"Error fetching data: {e}")
        finally:
            conn.close()
    else:
        flash("Could not connect to database.")
    
    return render_template('index.html', data=data)

@app.route('/import', methods=['POST'])
def import_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        if data_manager.import_data(filepath):
            flash('Data imported successfully!')
        else:
            flash('Failed to import data.')
        
        # Clean up uploaded file
        try:
            os.remove(filepath)
        except:
            pass
            
    return redirect(url_for('index'))

@app.route('/export/<format_type>')
def export_file(format_type):
    if format_type == 'excel':
        filename = "export.xlsx"
    else:
        filename = f"export.{format_type}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    if data_manager.export_data(filepath, format_type):
        return send_file(filepath, as_attachment=True)
    else:
        flash('Failed to export data.')
        return redirect(url_for('index'))

@app.route('/import/stock', methods=['POST'])
def import_stock():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))
    
    if file:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        success, result = data_manager.import_stock_data(filepath)
        
        if success:
            flash(f"Stock import complete. Processed {result['success_count']} records.")
            if result['errors']:
                for error in result['errors']:
                    flash(f"Warning: {error}")
        else:
            # If success is False, result is a list of errors
            flash(f"Failed to import stock data: {result[0]}")
        
        # Clean up uploaded file
        try:
            os.remove(filepath)
        except:
            pass
            
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
