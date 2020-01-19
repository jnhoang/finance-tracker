import json
import os
import uuid

TRACKED_CATEGORIES = {
  'shared': [
    'Joint Fast Food',
    'Joint Restaurant',
    'Grouceries',
    'Utilities',
    'Join Desserts',
    'Join Coffee Shop',
    'Movies & DVDs',
  ],

  'work': [
    'Work Fast Food',
    'Work Restaurant',
  ]
}



def main():
  header_cols, raw_rows =  convert_transactions()
  years_to_track        =  ['all-years', '2016', '2017', '2018', '2019', '2020']

  for year in years_to_track:
    output_json   =  process_data(header_cols, raw_rows, year)
    json_filename =  f'artifacts/json/{year}-output.json'
    write_json_to_file(json_filename, output_json)

    # process shared_expenses
    csv_filename = f'artifacts/csv/{year}-shared_expenses.csv'
    write_shared_expenses_to_file(json_filename, csv_filename)


def process_data(header_cols: list, raw_rows: list, year: str):
  if year != 'all-years':
    raw_rows = [row for row in raw_rows if get_year_from_date(row['date']) == year]
  unaccounted_rows = [ row for row in raw_rows]

  shared_rows =  [ row for row in raw_rows if row['category'] in TRACKED_CATEGORIES['shared'] ]
  work_rows   =  [ row for row in raw_rows if row['category'] in TRACKED_CATEGORIES['work'] ]

  # unaccounted rows
  shared_row_ids   =  [row['row_id'] for row in shared_rows]
  work_row_ids     =  [row['row_id'] for row in work_rows]
  unaccounted_rows =  [row for row in raw_rows if row['row_id'] not in shared_row_ids]
  unaccounted_rows =  [row for row in raw_rows if row['row_id'] not in work_row_ids]

  return {
    'shared_rows'      :  shared_rows,
    'work_rows'        :  work_rows,
    'header_cols'      :  header_cols,
    'raw_rows'         :  raw_rows,
    'unaccounted_rows' :  unaccounted_rows,
  }


def convert_transactions():
  with open('transactions.csv') as f:
    header_cols =  convert_line_to_dict(f.readline())
    raw_rows    =  [ convert_line_to_dict(line) for line in f ]
    raw_rows.reverse()

  return header_cols, raw_rows


def convert_line_to_dict(file_line: str):
  '''
    processes lines from a file and removes extra quotes
    returns a dict representing 1 transaction
  '''

  raw_cols =  file_line.split(',')
  cols     =  [ col.strip('"') for col in raw_cols ]
  row_data =  {
    'row_id'               :  str(uuid.uuid1()),
    'date'                 :  cols[0],
    'description'          :  str.lower(cols[1]),
    # 'original-description' :  cols[2],
    'amount'               :  cols[3],
    # 'transaction-type'     :  cols[4],
    'category'             :  cols[5],
    # 'account-name'         :  cols[6],
    # 'labels'               :  cols[7],
    'notes'                :  cols[8],
  }

  return row_data


def get_year_from_date(date):
  date = date.split('/')
  year = date[2]
  return year


def write_json_to_file(filename, json_data):
  with open(filename, 'w') as outfile:
    json.dump(json_data, outfile, sort_keys=True, indent=2)
  return


def write_shared_expenses_to_file(input_file, output_file):
  cols_to_write = ['date', 'description', 'amount']

  with open(input_file, 'r') as f:
    data = json.load(f)

  with open(output_file, 'w') as f:
    # copy headers
    for vals in cols_to_write:
      f.write(f"{data['header_cols'][vals]}, ")
    f.write('\n')
    # copy expenses
    for row in data['shared_rows']:
      for vals in cols_to_write:
        f.write(f"{row[vals]}, ")
      f.write('\n')



if __name__ == '__main__':
  main()
