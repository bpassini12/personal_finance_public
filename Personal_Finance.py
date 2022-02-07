import os
import bp_sql as bp
import pickle
import imaplib
import yagmail
import email
import datetime
from personalcapital import PersonalCapital, RequireTwoFactorException, TwoFactorVerificationModeEnum
import pandas as pd
from cryptography.fernet import Fernet
import json

pc_acct_map_df = pd.read_excel(io='personal_finance.xls', sheet_name='pc_acct_map')

target_db = 'insert_target_db_name.db'
target_tbl = 'target_tbl_name'

create_pf_tbl_sql = ''' Create Table if not exists target_tbl_name (
                                                                    date text,
                                                                    userAccountId integer,
                                                                    accountName text,
                                                                    ticker text,
                                                                    description text,
                                                                    holdingType text,
                                                                    acctType text,
                                                                    quantity real,
                                                                    price real,
                                                                    value real,
                                                                    fundFees real
                                                                ); '''

def send_email(email_subject, email_contents):
    '''used to send out final email'''

    gmail_user, gmail_pwd, a, b = get_credentials()
    
    yag = yagmail.SMTP(user=gmail_user, password=gmail_pwd)
    
    yag.send(to=gmail_user, subject=email_subject, contents=email_contents)

def get_credentials():
    '''gets encrypted login info'''
    
    key_path = os.path.join(os.path.expanduser('~'), '.fernet')
    key = pickle.load(open(key_path, 'rb'))
    cipher_suite = Fernet(key)
    encrypted_credentials_df = pd.read_csv('encrypted_credentials.csv')
    
    #gmail
    gmail = 'Gmail'
    gmail_ec_row = encrypted_credentials_df.loc[encrypted_credentials_df.login_account == gmail]
    gmail_user = gmail_ec_row.iloc[0]['username']
    gmail_pwd_encrypt = gmail_ec_row.iloc[0]['encrypted_password']
    gmail_pwd = cipher_suite.decrypt(str.encode(gmail_pwd_encrypt)).decode('utf-8')
    
    #pc
    pc = 'Personal Capital'
    pc_ec_row = encrypted_credentials_df.loc[encrypted_credentials_df.login_account == pc]
    pc_user = pc_ec_row.iloc[0]['username']
    pc_pwd_encrypt = pc_ec_row.iloc[0]['encrypted_password']
    pc_pwd = cipher_suite.decrypt(str.encode(pc_pwd_encrypt)).decode('utf-8')
    
    return gmail_user, gmail_pwd, pc_user, pc_pwd
    
def get_secure_auth_code(gmail_username, gmail_password):
    '''pc api has 2 factor auth, so you have to enter 4 diget code that's sent to email. 
       This func gets the 4 diget code from the email'''

    smtp_server = 'imap.gmail.com'
    today = datetime.date.today().strftime("%d-%b-%Y")
    
    mail = imaplib.IMAP4_SSL(smtp_server)
    mail.login(gmail_username, gmail_password)
    mail.select('inbox')
    
    data = mail.search(None, '(FROM "Personal Capital" SUBJECT "Register A New Computer" SINCE "' + today + '")')
    mail_ids = data[1]
    id_list = mail_ids[0].split()
    email_id = int(id_list[-1])
    res, msg = mail.fetch(str(email_id), '(RFC822)')
    
    for response in msg:
        if isinstance(response, tuple):
            msg = email.message_from_bytes(response[1])

            if msg.is_multipart():
                # iterate over email parts
                for part in msg.walk():
                    # extract content type of email
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))
                    try:
                        # get the email body
                        body = part.get_payload(decode=True).decode()
                    except:
                        pass
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        # print text/plain emails and skip attachments
                        b_split = body.split()
                        code = [i for i in b_split if len(i)==4 and i.isdigit()]
                        code = code[0]
            
                        return code

def get_pc_json(pc_username, pc_password, gmail_username, gmail_password):
    '''this logs into pc and requests api for json data'''
    
    pc = PersonalCapital()

    try:
        pc.login(pc_username, pc_password)
    except RequireTwoFactorException:
        pc.two_factor_challenge(TwoFactorVerificationModeEnum.EMAIL)
        
        #loops until it finds the code or until it cant find it 50 times
        endTime = datetime.datetime.now() + datetime.timedelta(minutes = 5)
 
        while datetime.datetime.now() <= endTime:
            try:
                code = get_secure_auth_code(gmail_username, gmail_password)
                pc.two_factor_authenticate(TwoFactorVerificationModeEnum.SMS, code)
                pc.authenticate_password(pc_username, pc_password)
                break
            except:
                continue

    #accounts_response = pc.fetch('/newaccount/getAccounts')
    #accounts = accounts_response.json()

    holdings_response = pc.fetch('/invest/getHoldings')
    holdings = holdings_response.json()

    return holdings

def format_df(json):
    '''formats json data into df'''

    df = pd.DataFrame()

    for i in json['spData']['holdings']:
        df = df.append(i, ignore_index=True)
        
    df['date'] = datetime.date.today().strftime("%m/%d/%Y")
    df.loc[:,'userAccountId'] = df.loc[:,'userAccountId'].astype(int)
    df.loc[:, 'fundFees'] = df.loc[:, 'fundFees'].fillna(0)
    df.loc[:, 'fundFees'] = df.loc[:, 'fundFees'].round(decimals=4)
    df = df.merge(pc_acct_map_df, how='left', on='userAccountId')
    bp_holdings_df = df[['date', 'userAccountId', 'accountName', 'ticker', 'description', 'holdingType', 'acctType', 'quantity', 'price', 'value', 'fundFees']]
    bp_holdings_df = bp_holdings_df.copy()
    bp_holdings_df.loc[bp_holdings_df[bp_holdings_df['ticker'] == 'NRG@']['ticker'].index , 'ticker'] = 'NRG'
    bp_holdings_df['date'] = pd.to_datetime(bp_holdings_df['date'])
    
    return bp_holdings_df

def append_new_df_to_tbl(df, tbl, db):
    '''check if new data date is greater than max date in db, if so append to db tbl'''

    bp.create_table(db_name=target_db, create_table_sql=create_pf_tbl_sql)

    conn = bp.create_connection(db_name=db)

    format_str = '%Y-%m-%d %H:%M:%S'
    check_date = datetime.datetime.strptime(pd.read_sql_query(con=conn, sql='''select max(date) as date from ''' + tbl).loc[0, 'date'], format_str).date()
    tday = datetime.date.today()
    
    if tday > check_date:
        df.to_sql(name=tbl, con=conn, if_exists='append', index=False)
        conn.close()



###main run

#get credentials
gmail_user, gmail_pwd, pc_user, pc_pwd = get_credentials()

#gets pc json data
holdings_json = get_pc_json(pc_username=pc_user, pc_password=pc_pwd, gmail_username=gmail_user, gmail_password=gmail_pwd)

#formats json data to df
bp_holdings_df = format_df(json=holdings_json)

#appends data to db tbl
append_new_df_to_tbl(df=bp_holdings_df, tbl=target_tbl, db=target_db)

#this is like access compact and compair but for sqlite db
bp.vacuum_db(db_name=target_db)


#rest formats a email to be sent out
email_df = bp_holdings_df.groupby(['date','holdingType','acctType'])['value'].sum().reset_index()
email_df.sort_values(by=['acctType','holdingType'], inplace=True)

email_df['value'] = email_df['value'].astype(int)
total_value = "${:,.0f}".format(email_df['value'].sum())

email_df['value'] = email_df.apply(lambda x: "{:,}".format(x['value']), axis=1)


today = datetime.date.today().strftime("%m/%d/%y")
send_email(email_subject = 'Personal Finance ' + today, email_contents=['Total Portfolio Value: ' + str(total_value) + '\n\n', email_df])

