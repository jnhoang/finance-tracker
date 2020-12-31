import json, uuid
from db_connector import DbConnector



def main():
  header_cols, raw_rows =  convert_transactions_to_json()
  print('header_cols:', header_cols)
  print('raw_rows:', json.dumps(raw_rows[0], indent=2))

  sql = "INSERT INTO transactions (date, description, amount, category, labels, notes, year) VALUES\n"

  # format rows into the following format
  """
    INSERT INTO transactions (date, description, amount, category, labels, notes)
    (DATE, DESCRIPTION, AMOUNT, CATEGORY, LABELS, NOTES)
  """
  insert_lines = [ "  ('%s', '%s', %s, '%s', '%s', '%s', '%s')" % (row['date'], row['description'], row['amount'], row['category'], row['labels'], row['notes'], row['year']) for row in raw_rows ]

  # build sql statements
  for line in insert_lines:
    line = line.replace('""', 'NULL')
    sql += f"{line},\n"

  # remove last ,
  sql = sql[:-2]
  with open('test.txt', 'w') as f:
    f.write(sql)

  # insert into database
  db = DbConnector()
  db.execute_sql(db.process_delete_results, 'DELETE FROM transactions;')
  db.execute_sql(db.process_insert_results, sql)


def convert_transactions_to_json():
  with open('transactions.csv') as f:
    header_cols =  convert_line_to_dict(f.readline())
    raw_rows    =  [ convert_line_to_dict(line) for line in f ]

  return header_cols, raw_rows


def convert_line_to_dict(file_line: str):
  '''
    processes lines from a file and removes extra quotes
    returns a dict representing 1 transaction
  '''
  raw_cols =  file_line.split('","')
  cols     =  [ col.strip('"') for col in raw_cols ]
  date     =  cols[0]
  year     =  date.split('/')[-1]

  row_data =  {
    'row_id'               :  str(uuid.uuid1()),
    'date'                 :  date,
    'year'                 :  year,
    'description'          :  str.lower(cols[1]).replace("'", ""),
    # 'original-description' :  cols[2],
    'amount'               :  cols[3],
    # 'transaction-type'     :  cols[4],
    'category'             :  str.lower(cols[5]),
    # 'account-name'         :  cols[6],
    'labels'               :  str.lower(cols[7]),
    'notes'                :  str.lower(cols[8]).replace('\n', '').replace("'", ""),
  }

  return row_data

if __name__ == '__main__':
  main()


