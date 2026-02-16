from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify
import data_manager
import config
import pandas as pd
import os
import uuid
import threading
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
            query = f"SELECT * FROM {config.TABLE_NAME} order by id desc limit 100"
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
    temp_dirs = []
    warnings = []

    try:
        # ========== Step 1: Simpan file & extract ZIP (sama seperti API) ==========
        for file in files:
            if file.filename == '':
                continue

            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            _, ext = os.path.splitext(filename)

            if ext.lower() == '.zip':
                temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], f'_zip_{os.path.splitext(filename)[0]}')
                os.makedirs(temp_dir, exist_ok=True)
                temp_dirs.append(temp_dir)

                extracted = data_manager.extract_zip(filepath, temp_dir)
                if extracted:
                    all_file_paths.extend(extracted)
                else:
                    msg = f"{filename}: Invalid ZIP or no data files found inside."
                    warnings.append(msg)
                    flash(f"⚠️ {msg}")

                try:
                    os.remove(filepath)
                except:
                    pass
            elif ext.lower() in ['.csv', '.txt']:
                all_file_paths.append(filepath)
            else:
                msg = f"{filename}: Unsupported file type '{ext}'. Skipped."
                warnings.append(msg)
                flash(f"⚠️ {msg}")
                try:
                    os.remove(filepath)
                except:
                    pass

        if not all_file_paths:
            flash('No valid data files to process.')
            return redirect(url_for('index'))

        # ========== Step 2: Quick validate (sama seperti API) ==========
        validation_results = []
        valid_files = []
        total_rows = 0

        for fp in all_file_paths:
            fname = os.path.basename(fp)
            is_valid, error_msg, row_count = data_manager.quick_validate_file(fp, table_name)
            if is_valid:
                valid_files.append(fp)
                total_rows += row_count
                validation_results.append({"filename": fname, "valid": True, "rows": row_count})
            else:
                validation_results.append({"filename": fname, "valid": False, "error": error_msg})

        if not valid_files:
            # Semua file gagal validasi -> bersihkan & tampilkan pesan
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

            flash('All files failed validation.')
            for v in validation_results:
                if not v['valid']:
                    flash(f"❌ {v['filename']}: {v.get('error', 'Validation failed.')}")
            return redirect(url_for('index'))

        # Tampilkan hasil validasi per file (warning untuk yang gagal)
        for v in validation_results:
            if v['valid']:
                flash(f"✅ {v['filename']}: {v['rows']} rows, validation passed.")
            else:
                flash(f"❌ {v['filename']}: {v.get('error', 'Validation failed.')}")

        # ========== Step 3: Buat job batch ==========
        batch_id = str(uuid.uuid4())
        filenames = [os.path.basename(fp) for fp in valid_files]
        data_manager.create_import_job(batch_id, ', '.join(filenames), table_name)
        data_manager.update_job_status(batch_id, total_rows=total_rows)

        # ========== Step 4: Jalankan proses async ==========
        thread = threading.Thread(
            target=data_manager.process_import_async,
            args=(valid_files, table_name, batch_id, temp_dirs),
            daemon=True
        )
        thread.start()

        flash(f"📦 Import job created with ID: {batch_id}.")
        flash(f"📊 Total rows to process: {total_rows} from {len(valid_files)} file(s).")
        flash("ℹ️ Data is being processed in background. You can check job status via API /api/jobs or /api/jobs/<batch_id>.")

        return redirect(url_for('index'))

    except Exception as e:
        # Cleanup on unexpected error
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

        flash(f"Server error during import: {str(e)}")
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


# ==================== REST API ENDPOINTS ====================

@app.route('/api/import', methods=['POST'])
def api_import_file():
    """API: Upload files, quick validate, then process async. Returns batch_id."""
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        return jsonify({"success": False, "error": "No files provided."}), 400

    table_name = request.form.get('table_name', 'auto')
    all_file_paths = []
    temp_dirs = []
    warnings = []
    filenames = []

    try:
        # Step 1: Save files and extract ZIPs
        for file in files:
            if file.filename == '':
                continue

            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            _, ext = os.path.splitext(filename)

            if ext.lower() == '.zip':
                temp_dir = os.path.join(app.config['UPLOAD_FOLDER'], f'_zip_{os.path.splitext(filename)[0]}')
                os.makedirs(temp_dir, exist_ok=True)
                temp_dirs.append(temp_dir)

                extracted = data_manager.extract_zip(filepath, temp_dir)
                if extracted:
                    all_file_paths.extend(extracted)
                else:
                    warnings.append(f"{filename}: Invalid ZIP or no data files found inside.")

                try:
                    os.remove(filepath)
                except:
                    pass
            elif ext.lower() in ['.csv', '.txt']:
                all_file_paths.append(filepath)
            else:
                warnings.append(f"{filename}: Unsupported file type '{ext}'. Skipped.")
                try:
                    os.remove(filepath)
                except:
                    pass

        if not all_file_paths:
            return jsonify({"success": False, "error": "No valid data files to process.", "warnings": warnings}), 400

        # Step 2: Quick validate each file (Phase 1)
        validation_results = []
        valid_files = []
        total_rows = 0

        for fp in all_file_paths:
            fname = os.path.basename(fp)
            is_valid, error_msg, row_count = data_manager.quick_validate_file(fp, table_name)
            if is_valid:
                valid_files.append(fp)
                total_rows += row_count
                validation_results.append({"filename": fname, "valid": True, "rows": row_count})
            else:
                validation_results.append({"filename": fname, "valid": False, "error": error_msg})

        if not valid_files:
            # All files failed validation — cleanup and return errors
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
            return jsonify({
                "success": False,
                "error": "All files failed validation.",
                "validation": validation_results,
                "warnings": warnings
            }), 400

        # Step 3: Create batch job
        batch_id = str(uuid.uuid4())
        filenames = [os.path.basename(fp) for fp in valid_files]
        data_manager.create_import_job(batch_id, ', '.join(filenames), table_name)
        data_manager.update_job_status(batch_id, total_rows=total_rows)

        # Step 4: Start background thread (Phase 2)
        thread = threading.Thread(
            target=data_manager.process_import_async,
            args=(valid_files, table_name, batch_id, temp_dirs),
            daemon=True
        )
        thread.start()

        # Return immediately with batch_id
        return jsonify({
            "success": True,
            "data": {
                "batch_id": batch_id,
                "status": "pending",
                "total_rows": total_rows,
                "files": filenames,
                "validation": validation_results,
                "warnings": warnings,
                "message": "Import job started. Use GET /api/jobs/<batch_id> to check progress."
            }
        }), 202

    except Exception as e:
        # Cleanup on unexpected error
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
        return jsonify({"success": False, "error": f"Server error: {str(e)}"}), 500


@app.route('/api/jobs', methods=['GET'])
def api_get_jobs():
    """API: List all import jobs."""
    limit = request.args.get('limit', 50, type=int)
    jobs = data_manager.get_all_jobs(limit=limit)
    return jsonify({"success": True, "data": jobs}), 200


@app.route('/api/jobs/<batch_id>', methods=['GET'])
def api_get_job(batch_id):
    """API: Get status of a specific import job by batch_id."""
    job = data_manager.get_job(batch_id)
    if job:
        return jsonify({"success": True, "data": job}), 200
    else:
        return jsonify({"success": False, "error": "Job not found."}), 404


@app.route('/api/jobs/<batch_id>/details', methods=['GET'])
def api_get_job_details(batch_id):
    """API: Get per-file details for a specific import job."""
    details = data_manager.get_job_file_details(batch_id)
    return jsonify({"success": True, "data": details}), 200


@app.route('/api/tables', methods=['GET'])
def api_get_tables():
    """API: List all import tables."""
    tables = data_manager.get_import_tables()
    return jsonify({"success": True, "data": tables}), 200


@app.route('/api/tables', methods=['POST'])
def api_create_table():
    """API: Create a new import table. Expects JSON body."""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON body required."}), 400

    table_name = data.get('table_name')
    display_name = data.get('display_name')
    allowed_filename = data.get('allowed_filename', '')
    columns = data.get('columns', [])  # [{"name": "col", "type": "str"}]

    if not table_name or not display_name:
        return jsonify({"success": False, "error": "table_name and display_name are required."}), 400

    if data_manager.create_new_import_table(table_name, display_name, columns, allowed_filename):
        return jsonify({"success": True, "data": {"message": f"Table '{display_name}' created."}}), 201
    else:
        return jsonify({"success": False, "error": "Failed to create table. Name might be duplicate."}), 400


@app.route('/api/tables/<table_name>/columns', methods=['GET'])
def api_get_columns(table_name):
    """API: Get column configs + aliases for a table."""
    configs = data_manager.get_column_configs(table_name=table_name)
    return jsonify({"success": True, "data": configs}), 200


@app.route('/api/tables/<table_name>/columns', methods=['POST'])
def api_add_column(table_name):
    """API: Add a column to an existing table. Expects JSON body."""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON body required."}), 400

    column_name = data.get('column_name')
    data_type = data.get('data_type', 'str')

    if not column_name:
        return jsonify({"success": False, "error": "column_name is required."}), 400

    if data_manager.add_column_to_table(table_name, column_name, data_type):
        return jsonify({"success": True, "data": {"message": f"Column '{column_name}' added to {table_name}."}}), 201
    else:
        return jsonify({"success": False, "error": "Failed to add column."}), 400


@app.route('/api/tables/<int:table_id>/filename', methods=['PUT'])
def api_update_filename(table_id):
    """API: Update allowed_filename for a table. Expects JSON body."""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON body required."}), 400

    allowed_filename = data.get('allowed_filename', '')

    if data_manager.update_allowed_filename(table_id, allowed_filename):
        return jsonify({"success": True, "data": {"message": "Allowed filename updated."}}), 200
    else:
        return jsonify({"success": False, "error": "Failed to update allowed filename."}), 400


@app.route('/api/columns/<int:column_id>', methods=['PUT'])
def api_update_column(column_id):
    """API: Update column config (is_mandatory, data_type). Expects JSON body."""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON body required."}), 400

    is_mandatory = data.get('is_mandatory', False)
    data_type = data.get('data_type', 'str')

    if data_manager.update_column_config(column_id, is_mandatory, data_type):
        return jsonify({"success": True, "data": {"message": "Column config updated."}}), 200
    else:
        return jsonify({"success": False, "error": "Failed to update column config."}), 400


@app.route('/api/columns/<int:column_id>/aliases', methods=['POST'])
def api_add_alias(column_id):
    """API: Add alias to a column. Expects JSON body."""
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON body required."}), 400

    alias_name = data.get('alias_name')
    if not alias_name:
        return jsonify({"success": False, "error": "alias_name is required."}), 400

    if data_manager.add_alias(column_id, alias_name):
        return jsonify({"success": True, "data": {"message": f"Alias '{alias_name}' added."}}), 201
    else:
        return jsonify({"success": False, "error": "Failed to add alias."}), 400


@app.route('/api/aliases/<int:alias_id>', methods=['DELETE'])
def api_delete_alias(alias_id):
    """API: Delete an alias by ID."""
    if data_manager.delete_alias(alias_id):
        return jsonify({"success": True, "data": {"message": "Alias deleted."}}), 200
    else:
        return jsonify({"success": False, "error": "Failed to delete alias."}), 400
    
@app.route('/batch')
def batch_page():
    jobs = data_manager.get_all_jobs(limit=100)
    return render_template('batch.html', jobs=jobs)


@app.route('/batch/<batch_id>')
def batch_detail_page(batch_id):
    job = data_manager.get_job(batch_id)
    if not job:
        flash("Job not found.", "error")
        return redirect(url_for('batch_page'))
    
    details = data_manager.get_job_file_details(batch_id)
    return render_template('job_details.html', job=job, details=details)


if __name__ == '__main__':
    app.run(debug=True)
