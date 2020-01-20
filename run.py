import json
import os
import uuid

TRACKED_CATEGORIES = {
  'shared': [
    'Joint Fast Food',
    'Joint Restaurant',
    'Groceries',
    'Utilities',
    'Joint Dessert',
    'Joint Coffee Shop',
    'Movies & DVDs',
    'Parking',

  ],

  'work': [
    'Work Fast Food',
    'Work Restaurant',
  ],

  'personal': [
    'Solo Coffeehouse',
    'Solo Restaurant',
    'Video Games',
    'Mortgage & Rent',
  ],

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

  shared_rows   , unaccounted_rows =  track_category(TRACKED_CATEGORIES['shared'], unaccounted_rows)
  work_rows     , unaccounted_rows =  track_category(TRACKED_CATEGORIES['work'], unaccounted_rows)
  personal_rows , unaccounted_rows =  track_category(TRACKED_CATEGORIES['personal'], unaccounted_rows)

  # log findings
  log_findings(
    year,
    raw_rows,
    shared_rows,
    work_rows,
    personal_rows,
    unaccounted_rows)

  # return
  return {
    'shared_rows'      :  shared_rows,
    'work_rows'        :  work_rows,
    'personal_rows'    :  personal_rows,
    'header_cols'      :  header_cols,
    'raw_rows'         :  raw_rows,
    'unaccounted_rows' :  unaccounted_rows,
  }


def track_category(category, unaccounted_rows):
  category_rows      =  []
  uncategorized_rows =  []

  for row in unaccounted_rows:
    if row['category'] in category:
      category_rows.append(row)
    else:
      uncategorized_rows.append(row)

  return category_rows, uncategorized_rows


def log_findings(year, raw_rows, shared_rows, work_rows, personal_rows, unaccounted_rows):
  print(f'\n\n=========== year: {year} ===========\n')
  print(f'num total_expenses    :  {len(raw_rows)}')
  print(f'num shared_expenses   :  {len(shared_rows)}')
  print(f'num work_expenses     :  {len(work_rows)}')
  print(f'num personal_expenses :  {len(personal_rows)}')
  print(f'num unaccounted rows  :  {len(unaccounted_rows)}')
  return


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
