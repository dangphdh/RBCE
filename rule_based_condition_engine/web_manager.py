from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from config_manager import ConfigManager
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)
config_manager = ConfigManager("config.yaml")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/datasets', methods=['GET', 'POST'])
def manage_datasets():
    if request.method == 'POST':
        dataset_name = request.form['name']
        base_table = request.form['base_table']
        
        if not dataset_name or not base_table:
            flash('Dataset name and base table are required.', 'error')
        elif dataset_name in config_manager.get_datasets():
            flash('Dataset with this name already exists.', 'error')
        else:
            dataset_config = {
                'base_table': base_table,
                'tables': [],
                'joins': []
            }
            config_manager.add_dataset(dataset_name, dataset_config)
            flash('Dataset added successfully.', 'success')
        return redirect(url_for('manage_datasets'))
    
    datasets = config_manager.get_datasets()
    return render_template('datasets.html', datasets=datasets)

@app.route('/scenarios', methods=['GET', 'POST'])
def manage_scenarios():
    if request.method == 'POST':
        scenario_name = request.form['name']
        dataset = request.form['dataset']
        
        if not scenario_name or not dataset:
            flash('Scenario name and dataset are required.', 'error')
        elif scenario_name in [s['name'] for s in config_manager.get_scenarios()]:
            flash('Scenario with this name already exists.', 'error')
        elif dataset not in config_manager.get_datasets():
            flash('Selected dataset does not exist.', 'error')
        else:
            scenario_config = {
                'name': scenario_name,
                'dataset': dataset,
                'output': {
                    'path': f'/path/to/output/{scenario_name}',
                    'mode': 'overwrite'
                },
                'conditions': [],
                'rules': []
            }
            config_manager.add_scenario(scenario_config)
            flash('Scenario added successfully.', 'success')
        return redirect(url_for('manage_scenarios'))
    
    scenarios = config_manager.get_scenarios()
    datasets = config_manager.get_datasets()
    return render_template('scenarios.html', scenarios=scenarios, datasets=datasets)

@app.route('/rules', methods=['GET', 'POST'])
def manage_rules():
    if request.method == 'POST':
        rule_id = request.form['rule_id']
        rule_name = request.form['name']
        scenario_name = request.form['scenario']
        output_columns = request.form['output_columns'].split(',')
        
        if not rule_id or not rule_name or not scenario_name or not output_columns:
            flash('All fields are required.', 'error')
        else:
            scenarios = config_manager.get_scenarios()
            for scenario in scenarios:
                if scenario['name'] == scenario_name:
                    if any(r['rule_id'] == rule_id for r in scenario['rules']):
                        flash('Rule with this ID already exists in the scenario.', 'error')
                    else:
                        scenario['rules'].append({
                            'rule_id': rule_id,
                            'name': rule_name,
                            'output_columns': output_columns
                        })
                        config_manager.update_scenario(scenario_name, scenario)
                        flash('Rule added successfully.', 'success')
                    break
            else:
                flash('Selected scenario does not exist.', 'error')
        return redirect(url_for('manage_rules'))
    
    rules = config_manager.get_rules()
    scenarios = config_manager.get_scenarios()
    return render_template('rules.html', rules=rules, scenarios=scenarios)

@app.route('/conditions', methods=['GET', 'POST'])
def manage_conditions():
    if request.method == 'POST':
        condition_id = request.form['condition_id']
        condition_name = request.form['name']
        scenario_name = request.form['scenario']
        condition_expr = request.form['condition_expr']
        
        if not condition_id or not condition_name or not scenario_name or not condition_expr:
            flash('All fields are required.', 'error')
        else:
            scenarios = config_manager.get_scenarios()
            for scenario in scenarios:
                if scenario['name'] == scenario_name:
                    if any(c['condition_id'] == condition_id for c in scenario['conditions']):
                        flash('Condition with this ID already exists in the scenario.', 'error')
                    else:
                        scenario['conditions'].append({
                            'condition_id': condition_id,
                            'name': condition_name,
                            'condition_expr': condition_expr
                        })
                        config_manager.update_scenario(scenario_name, scenario)
                        flash('Condition added successfully.', 'success')
                    break
            else:
                flash('Selected scenario does not exist.', 'error')
        return redirect(url_for('manage_conditions'))
    
    conditions = config_manager.get_conditions()
    scenarios = config_manager.get_scenarios()
    return render_template('conditions.html', conditions=conditions, scenarios=scenarios)

@app.route('/delete/<item_type>/<item_id>', methods=['POST'])
def delete_item(item_type, item_id):
    try:
        if item_type == 'scenario':
            config_manager.delete_scenario(item_id)
        elif item_type == 'rule':
            scenarios = config_manager.get_scenarios()
            for scenario in scenarios:
                scenario['rules'] = [r for r in scenario['rules'] if r['rule_id'] != item_id]
                config_manager.update_scenario(scenario['name'], scenario)
        elif item_type == 'condition':
            scenarios = config_manager.get_scenarios()
            for scenario in scenarios:
                scenario['conditions'] = [c for c in scenario['conditions'] if c['condition_id'] != item_id]
                config_manager.update_scenario(scenario['name'], scenario)
        elif item_type == 'dataset':
            config_manager.config['datasets'].pop(item_id, None)
            config_manager.save_config()
        else:
            return jsonify({'status': 'error', 'message': 'Invalid item type'}), 400
        
        flash(f'{item_type.capitalize()} deleted successfully.', 'success')
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)