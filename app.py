from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from datetime import datetime
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:postgres@localhost:5432/channels_db'
db = SQLAlchemy(app)
app.config['JSON_SORT_KEYS'] = False


class Channels(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    channel = db.Column(db.String(20))
    country = db.Column(db.String(2))
    os = db.Column(db.String(10))
    impressions = db.Column(db.Integer)
    clicks = db.Column(db.Integer)
    installs = db.Column(db.Integer)
    spend = db.Column(db.Float)
    revenue = db.Column(db.Float)

    def __repr__(self):
        return self.channel


def read_csv(file_name):
    csv_reader = pd.read_csv(file_name, low_memory=False).fillna('')
    raw_input = csv_reader.values
    for item in raw_input:
        c1 = Channels(date=datetime.strptime(item[0], '%Y-%m-%d'),
                      channel=item[1],
                      country=item[2],
                      os=item[3],
                      impressions=item[4],
                      clicks=item[5],
                      installs=item[6],
                      spend=item[7],
                      revenue=item[8])
        db.session.add(c1)
    db.session.commit()


@app.route('/', methods=['POST'])
def show_channels():
    global error_message
    error_message = 'Please Send at Least One Metrics'

    def check_parameter_type(param ,  param_name, member_type , allowed_values, mandatory=False):
        type_names ={str:'Strings', dict:'Dictionaries'}
        global error_message
        if type(param)!=list:
            error_message += f' Please sned list of {param_name} inside[]. '
        else:
            list_item_type = {type(item) for item in param if type(item) != member_type}
            if len(list_item_type):
                error_message += f" Please Sned Each {param_name} as a List of {type_names[member_type]} . "
                return
            if member_type==dict:
                try:
                    member_param = {item['name'] for item in param}
                    invalid_values = set(member_param) - allowed_values
                except:
                    error_message += f" Please Sned Name per each {param_name} Dictionary . "
                    return

            else:
                invalid_values = set(param) - allowed_values
            if len(invalid_values):
                error_message += f" Invalid {param_name}: {','.join(invalid_values)}. "
            if mandatory:
                if len(param)==0:
                    error_message += f" please Send at Least One {param_name}. "
        return error_message

    def check_metrics(metrics):
        allowed_values = {'impressions', 'clicks', 'installs', 'spend', 'revenue', 'CPI'}
        check_parameter_type(metrics, 'Metrics', str, allowed_values)

    def check_breakdowns(breakdowns):
        allowed_values = {'date', 'channel', 'country', 'os'}
        check_parameter_type(breakdowns, 'Breakdowns', str, allowed_values)

    def check_filters(filters):
        global error_message
        whr_clouse = ' 1 = 1 '
        if filters:
            allowed_values = {'date_from', 'date_to',  'date', 'channel', 'country', 'os'}
            check_parameter_type(filters, 'Filters', dict, allowed_values)

            if not error_message:
                for item in filters:
                    keys = list(item.keys())
                    if 'value' not in keys:
                        error_message+='Please Send Value per each Filter'
                        return
                    filter_name = item['name']
                    if item['name']=='date_from':
                        operand ='>='
                        filter_name = 'date'
                    elif item['name']=='date_to':
                        operand='<='
                        filter_name = 'date'
                    else:
                        operand='='
                    if item['name'] in ('date', 'date_from', 'date_to'):
                        whr_clouse += f" and {filter_name}   {operand}  '{item['value']}'"
                    else:
                        whr_clouse += f" and lower({filter_name})   {operand}  lower('{item['value']}')"
        return whr_clouse

    def check_sortings(sorting):
        global error_message
        sorting_string = ''
        if sorting:
            allowed_values = {'date', 'channel', 'country', 'os', 'impressions', 'clicks', 'installs', 'spend', 'revenue', 'CPI'}
            check_parameter_type(sorting, 'Sorting', dict, allowed_values)
            if not error_message:
                try:
                    sorting_type = {item['type'] for item in sorting if item['type'].lower() not in ('asc', 'desc')}
                    if len(sorting_type):
                        error_message += ' Invalid Sorting Type: ' + ','.join(sorting_type) + '. '
                except:
                    pass
                    # default sort ASC

                for item in sorting:
                    sorting_string += ',' +item['name'] + ' ' + item.get('type','')


                if sorting_string:
                    sorting_string =' order by '+sorting_string.strip(',')
        return sorting_string





    if request.data and json.loads(request.data).get('metrics'):
        error_message = ''

        record = json.loads(request.data)
        metrics = record.get('metrics') if record.get('metrics') else []
        check_metrics(metrics)

        filters = record.get('filters')
        whr_clouse = check_filters(filters)



        breakdowns = record.get('breakdowns') if record.get('breakdowns') else []
        check_breakdowns(breakdowns)

        sorting = record.get('sorting')
        sorting_string = check_sortings(sorting)


        if not error_message:
            select_columns = ','.join(  breakdowns + [
                'round(cast(sum(' + item + ') as decimal),2) as ' + item if item != 'CPI' else 'round(cast (sum(spend/installs) as decimal),2) as CPI'
                for item in metrics])
            sql_query = f"SELECT {select_columns} FROM channels where {whr_clouse} "
            columns = metrics
            if breakdowns:
                sql_query += ' group by ' + ','.join(breakdowns)
                columns = breakdowns + metrics
            if sorting:
                sql_query +=  sorting_string
            result = db.engine.execute(sql_query)
            result_as_list = result.fetchall()
            dict_result = [dict(zip(columns, row)) for row in result_as_list]
            result.close()
            return jsonify(dict_result), 200
    return jsonify({'error': error_message}), 404


if __name__ == '__main__':
    app.run(debug=True)
