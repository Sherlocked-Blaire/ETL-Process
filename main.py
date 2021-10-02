import numpy as np
import pandas as pd
import sqlite3


class DataTransformation:
  """Performs data loading and data transformation
  """
  def __init__(self, url:str) -> None:
    try:
      self.__data = pd.read_csv(url, sep=";")
      self.__transform()
    except Exception as error:
      raise error("There was a  problem loading the file", error)

  def __transform(self) -> None:
    column_names = {"fecha_nacimiento":"birth_date", "fecha_vencimiento":"due_date", 
                    "deuda":"due_balance","direccion":"address",
                    "correo":"email", "estatus_contacto":"status", 
                    "deuda":"due_balance","prioridad":"priority", "telefono":"phone"}
    try:
      self.__data = self.__data.rename(column_names, axis=1)
      #self.__data[['birth_date','due_date']] = self.__data[['birth_date','due_date']].apply(pd.to_datetime, format="%Y-%m-%d")
      self.__data['due_date'] = pd.to_datetime(self.__data['due_date'])
      self.__data['birth_date'] = pd.to_datetime(self.__data['birth_date'])
      today = pd.to_datetime("today")
      self.__data['delinquency'] =  (today - self.__data['due_date']).dt.days
      self.__data['age'] =  today.year - self.__data['birth_date'].dt.year
      self.__data['due_date'] = self.__data['due_date'].dt.date
      self.__data['birth_date'] = self.__data['birth_date'] .dt.date
      bins= [0,21,31,41,51,61,np.inf]
      labels = [1,2,3,4,5,6]
      self.__data['age_group'] = pd.cut( self.__data['age'] , bins=bins, labels=labels, right=False)
      self.__data[['first_name','last_name','gender','address','email','status']] = self.__data[['first_name','last_name',
                                                                              'gender','address','email','status']].applymap(
                                                                                  lambda x: str(x).upper())
    except Exception as error:
      raise error
      
  def extract_emails(self) -> pd.DataFrame:
    """Extracts email data in a dataframe from  data
    """                                                                      
    return self.__data[['fiscal_id','email','status','priority']]
      
  def extract_phone_numbers(self) -> pd.DataFrame: 
    """Extracts phone data in a dataframe from  data
    """  
    return self.__data[['fiscal_id','phone','status','priority']]
      
  def extract_customer_details(self) -> pd.DataFrame:
    """Extracts customer data in a dataframe from  data
    """ 
    return self.__data[['fiscal_id', 'first_name', 'last_name', 
                        'gender', 'birth_date','age', 'age_group',
                        'due_date', 'delinquency', 'due_balance', 'address']]
      
      
class Database: 
  def __init__(self) -> None:
    self.__connection = sqlite3.connect("database.db3")

  def create_tables(self) -> None:
    """Creates tables in the sqlite database
    """
    create_customers_table = """CREATE TABLE IF NOT EXISTS customers(fiscal_id TEXT PRIMARY KEY,
                          first_name TEXT ,
                          last_name TEXT,
                          gender TEXT,
                          birth_date DATE,
                          age INTEGER,
                          age_group INTEGER,
                          due_date DATE,
                          delinquency INTEGER,
                          due_balance INTEGER,
                          address TEXT)
                          """
    create_emails_table ="""CREATE TABLE IF NOT EXISTS emails(fiscal_id TEXT  PRIMARY KEY,
                          email TEXT,
                          status TEXT,
                          priority INTEGER,
                          FOREIGN KEY(fiscal_id)REFERENCES customers(fiscal_id) )
                        """
    create_phones_table = """CREATE TABLE IF NOT EXISTS phones(fiscal_id TEXT PRIMARY KEY, 
                          phone TEXT,
                          status TEXT,
                          priority INTEGER, 
                          FOREIGN KEY(fiscal_id)REFERENCES customers(fiscal_id) )
                          """
    self.__cursor = self.__connection.cursor()
    try:
      self.__cursor.execute(create_customers_table)
      self.__cursor.execute(create_emails_table)
      self.__cursor.execute(create_phones_table)
      print("Tables successfully created in database.db3")
    except Exception as error:
      raise error
      
  def save_customers_data_to_excel(self, customers:pd.DataFrame, emails:pd.DataFrame, phones:pd.DataFrame) -> None:
    """saves customer, emails and phones dataframe to excel in output directory
    """
    try:
      customers.to_excel("output/clientes.xlsx", index= False)
      emails.to_excel("output/emails.xlsx", index= False)
      phones.to_excel("output/phones.xlsx", index= False)
      print("Data successfully saved to Excel")
    except Exception as error:
      raise error
    
  def save_customers_data_to_database(self, customers:pd.DataFrame, emails:pd.DataFrame, phones:pd.DataFrame) -> None:
    """ saves customer, emails and phone data into respective tables in database 
    """
    try:
      customers.to_sql('customers', self.__connection, if_exists='append', index=False)
      emails.to_sql('emails', self.__connection, if_exists='append', index=False)
      phones.to_sql('phones', self.__connection, if_exists='append', index=False)
      print("Data successfully saved to SQL database")
    except Exception as error:
      raise error
                                                                                    
                                                                                        
if __name__ == '__main__':
  path = input("Please enter the path to the file you want to read csv from: ")
  data = DataTransformation(path)
  phones = data.extract_phone_numbers()
  emails = data.extract_emails()
  customers = data.extract_customer_details()
  db = Database()
  db.create_tables()
  db.save_customers_data_to_excel(customers=customers, phones=phones, emails=emails)
  db.save_customers_data_to_database(customers=customers, phones=phones, emails=emails)    
