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
    # Default to stocks if not provided
    current_table = request.args.get('table', 'stocks')
    tables = data_manager.get_import_tables()
    configs = data_manager.get_column_configs(table_name=current_table)
    return render_template('config.html', configs=configs, current_table=current_table, tables=tables)

@app.route('/config/update', methods=['POST'])
def update_config():
    column_id = request.form.get('id')
    is_mandatory = request.form.get('is_mandatory') == 'on'
    data_type = request.form.get('data_type')
    table_name = request.form.get('table_name', 'stocks') # Need to pass this back for redirect
    
    if data_manager.update_column_config(column_id, is_mandatory, data_type):
        flash("Configuration updated successfully.", "success")
    else:
        flash("Failed to update configuration.", "error")
    return redirect(url_for('config_page', table=table_name))

@app.route('/config/alias/add', methods=['POST'])
def add_alias():
    column_id = request.form.get('column_id')
    alias_name = request.form.get('alias_name')
    table_name = request.form.get('table_name', 'stocks')
    
    if alias_name and data_manager.add_alias(column_id, alias_name):
        flash("Alias added.", "success")
    else:
        flash("Failed to add alias.", "error")
    return redirect(url_for('config_page', table=table_name))

@app.route('/config/alias/delete/<int:alias_id>')
def delete_alias(alias_id):
    # To redirect back to the correct table, we might need to know which table the alias belonged to.
    # For now simplicity, we might lose the table context or need to look it up.
    # Or strict redirect to default. Let's try to get it from referrer or arg if possible.
    # Simpler: just redirect to config_page, default stocks. User can switch back.
    if data_manager.delete_alias(alias_id):
        flash("Alias deleted.", "success")
    else:
        flash("Failed to delete alias.", "error")
    return redirect(request.referrer or url_for('config_page'))

@app.route('/master-config')
def master_config():
    tables = data_manager.get_import_tables()
    return render_template('master_config.html', tables=tables)

@app.route('/config/add-table', methods=['POST'])
def add_table():
    display_name = request.form.get('display_name')
    table_name = request.form.get('table_name')
    allowed_filename = request.form.get('allowed_filename', '')
    
    col_names = request.form.getlist('col_name[]')
    col_types = request.form.getlist('col_type[]')
    
    initial_columns = []
    for name, dtype in zip(col_names, col_types):
        if name and name.strip():
            initial_columns.append({'name': name.strip(), 'type': dtype})
            
    if data_manager.create_new_import_table(table_name, display_name, initial_columns, allowed_filename):
        flash(f"Table '{display_name}' created successfully.", "success")
    else:
        flash("Failed to create table. Name might be duplicate.", "error")
        
    return redirect(url_for('master_config'))

@app.route('/config/update-filename', methods=['POST'])
def update_filename():
    table_id = request.form.get('table_id')
    allowed_filename = request.form.get('allowed_filename', '')
    
    if data_manager.update_allowed_filename(table_id, allowed_filename):
        flash("Allowed filename updated.", "success")
    else:
        flash("Failed to update allowed filename.", "error")
    return redirect(url_for('master_config'))

@app.route('/config/add-column', methods=['POST'])
def add_column():
    table_name = request.form.get('table_name')
    column_name = request.form.get('column_name')
    data_type = request.form.get('data_type')
    
    if data_manager.add_column_to_table(table_name, column_name, data_type):
        flash(f"Column '{column_name}' added to {table_name}.", "success")
    else:
        flash("Failed to add column.", "error")
        
    return redirect(url_for('master_config'))

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
    
    import_tables = data_manager.get_import_tables()
    return render_template('index.html', data=data, tables=import_tables)

@app.route('/import', methods=['POST'])
def import_file():
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        flash('No files selected')
        return redirect(url_for('index'))

    table_name = request.form.get('table_name', 'auto')
    all_file_paths = []
    temp_dirs = []  # track temp dirs for cleanup

    try:
        for file in files:
            if file.filename == '':
                continue

            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            _, ext = os.path.splitext(filename)

            if ext.lower() == '.zip':
                # Extract ZIP to temp directory
                temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], f'_zip_{os.path.splitext(filename)[0]}')
                os.makedirs(temp_dir, exist_ok=True)
                temp_dirs.append(temp_dir)

                extracted = data_manager.extract_zip(filepath, temp_dir)
                if extracted:
                    all_file_paths.extend(extracted)
                else:
                    flash(f"⚠️ {filename}: Invalid ZIP file or no data files (.csv/.txt) found inside.")

                # Remove the zip file itself
                try:
                    os.remove(filepath)
                except:
                    pass
            elif ext.lower() in ['.csv', '.txt']:
                all_file_paths.append(filepath)
            else:
                flash(f"⚠️ {filename}: Unsupported file type '{ext}'. Skipped.")
                try:
                    os.remove(filepath)
                except:
                    pass

        if not all_file_paths:
            flash('No valid data files to process.')
            return redirect(url_for('index'))

        # Process all collected files
        results = data_manager.import_multiple_files(all_file_paths, table_name)

        # Flash results per file
        success_total = 0
        fail_total = 0
        for r in results:
            if r['success']:
                success_total += 1
                flash(f"✅ {r['filename']}: {r['message']}")
                if r.get('errors'):
                    for err in r['errors']:
                        flash(f"⚠️ {r['filename']}: {err}")
            else:
                fail_total += 1
                flash(f"❌ {r['filename']}: {r['message']}")

        flash(f"📊 Import Summary: {success_total} file(s) succeeded, {fail_total} file(s) failed out of {len(results)} total.")

    finally:
        # Cleanup all uploaded/extracted files
        for fp in all_file_paths:
            try:
                os.remove(fp)
            except:
                pass
        for td in temp_dirs:
            try:
                import shutil
                shutil.rmtree(td, ignore_errors=True)
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
    
    # Export currently hardcoded to stocks table in data_manager.export_data
    # For now, leaving as is unless user asked for sales export (they didn't).
    if data_manager.export_data(filepath, format_type):
        return send_file(filepath, as_attachment=True)
    else:
        flash('Failed to export data.')
        return redirect(url_for('index'))

# Removed separate /import/stock route, merged into generic /import above


if __name__ == '__main__':
    app.run(debug=True)
