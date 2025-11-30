import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
import sys

def excel_to_mysql(excel_file, db_config):
    connection_string = (
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    engine = create_engine(connection_string)
    
    print(f"Reading Excel file: {excel_file}")
    
    if excel_file.endswith('.xlsb'):
        all_sheets = pd.read_excel(excel_file, sheet_name=None, engine='pyxlsb')
    else:
        all_sheets = pd.read_excel(excel_file, sheet_name=None)
    
    print(f"Found {len(all_sheets)} sheets")
    
    for sheet_name, df in all_sheets.items():
        table_name = "data_mes"
        
        print(f"\nImporting sheet '{sheet_name}' to table '{table_name}'")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # if_exists options: 'fail', 'replace', 'append'
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000
        )
        
        with engine.connect() as conn:
            conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `id` INT AUTO_INCREMENT PRIMARY KEY FIRST"))
            conn.commit()
        
        print(f"  ✓ Successfully imported to table '{table_name}' with auto-increment ID")
    
    print(f"\n✓ All {len(all_sheets)} sheets imported successfully!")
    engine.dispose()


if __name__ == "__main__":
    db_config = {
        'host': '127.0.0.1',     
        'port': 3306,            
        'user': 'root',  
        'password': '',  
        'database': 'mes'
    }
    
    excel_file = 'yourexcel.xlsx'
    
    try:
        excel_to_mysql(excel_file, db_config)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)