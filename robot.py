import os
import hashlib
import hmac
from base64 import b64encode
from datetime import timedelta, date
import json

import pandas as pd
import requests
from config import *


import time


## print 1
print('start')

test = True
pd.options.mode.chained_assignment = None

start = time.time()
log_path = 'files/log.txt'
drive_path = 'files/updates/'


today_str = date.today().strftime("%Y-%m-%d")
yesterday_str = (date.today() - timedelta(1)).strftime("%Y-%m-%d")

file_path = f'files/{today_str}'

if not os.path.exists(file_path):
    os.makedirs(file_path)

accumulator_url = 'files/accumulator.csv'
accumulator = pd.read_csv(accumulator_url)


def generate_hash(raw_data: str, secret: str):
    secret_key = bytearray(secret, 'utf-8')
    thing_to_hash = raw_data.encode('utf-8')
    return hmac.new(secret_key, msg=thing_to_hash, digestmod=hashlib.sha256).hexdigest()

def generate_random_data():
    random_bytes = os.urandom(64)
    return b64encode(random_bytes).decode('utf-8')

def check_ping(stone_code: str, serial_number: str, headers: dict):
    try:
        res = requests.get(url=f'{TOTH_URL}/{stone_code}/{serial_number}', headers=headers, timeout=10)
        jsonifyied = json.loads(res.text)
        return jsonifyied['CCID']
    except:
        return None

def get_ccid_in(df: pd.DataFrame):
    if len(df) == 0:
        df['ccid_in'] = None
        return df

    df['ccid_in'] = df[['customer', 'installedTerminalSerialNumber']]\
    .apply(lambda row: check_ping(str(row['customer']), str(row['installedTerminalSerialNumber']).upper(), toth_headers),\
           axis=1)
    return df

def get_ccid_out(df: pd.DataFrame):
    if len(df) == 0:
        df['ccid_out'] = None
        return df

    df['ccid_out'] = df[['customer', 'uninstalledTerminalSerialNumber']]\
    .apply(lambda row: check_ping(str(row['customer']), str(row['uninstalledTerminalSerialNumber']).upper(), toth_headers),\
           axis=1)
    return df

def wkf_request(ccid: object, WKF_URL: object, WKF_USER: object, WKF_PASS: object, provider: object, status: object):
    querystring = {"singleWsdl": ""}

    wkf_payload = f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:tem=\"http://tempuri.org/\" xmlns:wkf=\"http://schemas.datacontract.org/2004/07/WkfDomain.IntegrationStone.Models.WSModel\">\r\n   <soapenv:Header/>\r\n   <soapenv:Body>\r\n      <tem:WSAtualizaEstoque>\r\n         <!--Optional:-->\r\n         <tem:auth>\r\n            <!--Optional:-->\r\n            <wkf:Password>{WKF_PASS}</wkf:Password>\r\n            <!--Optional:-->\r\n            <wkf:UserName>{WKF_USER}</wkf:UserName>\r\n         </tem:auth>\r\n         <!--Optional:-->\r\n         <tem:atualizaEstoque>\r\n            <!--Optional:-->\r\n            <wkf:NumeroSerie>{ccid}</wkf:NumeroSerie>\r\n            <!--Optional:-->\r\n            <wkf:Prestador>{provider}</wkf:Prestador>\r\n            <!--Optional:-->\r\n            <wkf:Situacao>{status}</wkf:Situacao>\r\n        </tem:atualizaEstoque>\r\n      </tem:WSAtualizaEstoque>\r\n   </soapenv:Body>\r\n</soapenv:Envelope>"


    headers_wkf = {
        'Content-Type': "text/xml",
        'SOAPAction': "http://tempuri.org/IWKFServiceLogistic/WSAtualizaEstoque",
        'User-Agent': "PostmanRuntime/7.13.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "77ae63a5-f8f7-4f76-9beb-d5e8eb576c08,f5f95d29-5a80-413c-afcf-72af343039b1",
        'Host': "workfinitystonews.azurewebsites.net",
        'accept-encoding': "gzip, deflate",
        'content-length': "849",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    request_wkf = requests.request("POST", WKF_URL, data=wkf_payload.encode('utf-8'), headers=headers_wkf, params=querystring)

    if request_wkf.text.__contains__('Estoque atualizado com sucesso!'):
        return True
    else:
        return False

def change_status(df: pd.DataFrame, WKF_URL: object, WKF_USER: object, WKF_PASS: object):
    if len(df) == 0:
        df['response_wkf'] = None
        return df

    df['response_wkf'] = df.apply(lambda row: wkf_request(ccid=row['ccid'], WKF_URL=WKF_URL, WKF_USER=WKF_USER, WKF_PASS=WKF_PASS, provider=row['provider'], status=row['logistic_status']), axis=1)
    return df

def telecom_request(ccid: object, TELECOM_API_URL: object, TELECOM_API_TOKEN: object):

    querystring = {"ccid": ccid}
    headers = {
        'Authorization': f'"Bearer {TELECOM_API_TOKEN}"'
    }

    ## print bonus 
    print(headers['Authorization'])

    try:
        request_telecom = requests.request("GET", TELECOM_API_URL, headers=headers, params=querystring)
        print(request_telecom.text)
        status = json.loads(request_telecom.text)['data']['STATUS']
    except:
        status = None
    return status

def validate_sim(df: pd.DataFrame, TELECOM_API_URL: object, TELECOM_API_TOKEN: object):
    if len(df) == 0:
        df['telecom_status'] = None
        return df

    df['telecom_status'] = df.apply(lambda row: telecom_request(ccid=row['ccid'], TELECOM_API_URL=TELECOM_API_URL, TELECOM_API_TOKEN=TELECOM_API_TOKEN), axis=1)

    return df

random_str = generate_random_data()
hashed_str = generate_hash(raw_data=random_str, secret=CLIENT_SECRET_KEY)

toth_headers = {
    'ClientApplicationKey': CLIENT_APP_KEY,
    'RawData': random_str,
    'EncryptedData': hashed_str
}

wkf_headers = {
    "token": LOGISTIC_TOKEN
}

payload = {
    "date_end": today_str,
    "date_start": yesterday_str,
    "completed": True
}


## print 2
print('request wkf')

response_wkf = requests.get(url=LOGISTIC_URL, headers=wkf_headers, params=payload)
response_dict = json.loads(response_wkf.text)
df = pd.DataFrame(response_dict['result'])
df['date_check'] = today_str

new_instalacao = df[(df['serviceGroup'] == 'INSTALAÇÃO')\
                & (df['status'] == 'BAIXADA')\
                & (df['installedTerminalType'].isin(['GPRS-WIFI', 'GPRS', 'ANDROID']))]

instalacao = accumulator.append(other=new_instalacao, ignore_index=True, sort=False).drop_duplicates(subset='orderNumber')

desinstalacao = df[(df['serviceGroup'] == 'DESINSTALAÇÃO')\
                   & (df['status'] == 'BAIXADA')
                   & (df['uninstalledTerminalType'].isin(['GPRS-WIFI', 'GPRS', 'ANDROID']))]

troca = df[(df['serviceGroup'] == 'TROCA') & (df['status'] == 'BAIXADA')\
           & (df['solution'].isin(['Preferência por Troca de Equipamento', 'Troca do equipamento']))]

if test:
    instalacao = instalacao.head(n=50)
    desinstalacao = desinstalacao.head(n=50)
    troca = troca.head(n=50)

instalacao_ccids = get_ccid_in(instalacao)
desinstalacao_ccids = get_ccid_out(desinstalacao)
troca_ccid_in = get_ccid_in(troca)
troca_ccids = get_ccid_out(troca_ccid_in)

appendable_list = [instalacao_ccids[instalacao_ccids['ccid_in'].isnull()].drop(columns='ccid_in'),\
                   troca_ccids.loc[(troca_ccids['ccid_in'].isnull())\
                               & (troca_ccids['installedTerminalType'].isin(['GPRS-WIFI','GPRS','ANDROID']))]\
                       .drop(columns=['ccid_in', 'ccid_out'])]

accumulator = pd.concat(appendable_list)
accumulator.to_csv('files/accumulator.csv', index=False)

instalacao_ccids.to_csv(f'{file_path}/{today_str}_instalacao.csv', index=False)
desinstalacao_ccids.to_csv(f'{file_path}/{today_str}_desinstalacao.csv', index=False)
troca_ccids.to_csv(f'{file_path}/{today_str}_trocas.csv', index=False)

# Tratando instalacao
instalacao_ccids = instalacao_ccids[instalacao_ccids['ccid_in'].notnull()].rename(index=str, columns={'ccid_in': 'ccid'})
instalacao_ccids['logistic_status'] = 'EM PRODUÇÃO'
instalacao_df = instalacao_ccids[['ccid', 'logistic_status', 'customer', 'serviceGroup', 'provider', 'date_check',\
                                  'installedTerminalSerialNumber', 'orderNumber']]


# Tratando desinstalacao
desinstalacao_ccids = desinstalacao_ccids[desinstalacao_ccids['ccid_out'].notnull()].rename(index=str, columns={'ccid_out': 'ccid'})
desinstalacao_ccids['logistic_status'] = 'GOOD'
desinstalacao_df = desinstalacao_ccids[['ccid', 'logistic_status', 'customer', 'serviceGroup', 'provider', 'date_check',\
                                        'uninstalledTerminalSerialNumber', 'orderNumber']]

# Tratando trocas
troca_ccids_inst = troca_ccids[troca_ccids['ccid_in'].notnull()].rename(index=str, columns={'ccid_in': 'ccid'})
troca_ccids_inst['logistic_status'] = 'EM PRODUÇÃO'
troca_df_inst = troca_ccids_inst[['ccid', 'logistic_status', 'customer', 'serviceGroup', 'provider', 'date_check',\
                                  'installedTerminalSerialNumber', 'orderNumber']]


troca_ccids_desinst = troca_ccids[(troca_ccids['ccid_in'] != troca_ccids['ccid_out'])\
                                  & (troca_ccids['ccid_out'].notnull())].rename(index=str, columns={'ccid_out': 'ccid'})

troca_ccids_desinst['logistic_status'] = 'GOOD'
troca_df_desinst = troca_ccids_desinst[['ccid', 'logistic_status', 'customer', 'serviceGroup', 'provider', 'date_check',\
                                        'uninstalledTerminalSerialNumber', 'orderNumber']]



frames = [instalacao_df, desinstalacao_df, troca_df_inst, troca_df_desinst]


daily_log = pd.concat(frames, sort=False)


## print 3
print('api de telecom')

validated_daily_log = daily_log
#validated_daily_log = validate_sim(daily_log, TELECOM_API_URL, TELECOM_API_TOKEN)

#validated_daily_log = validated_daily_log.loc[validated_daily_log['telecom_status'].notnull()]
#validated_daily_log = validated_daily_log.loc[~validated_daily_log['telecom_status'].isin(['CANCELLED', 'SUSPENDED'])]



daily_log_wkf = change_status(df=validated_daily_log, WKF_URL=WKF_URL, WKF_USER=WKF_USER, WKF_PASS=WKF_PASS)


daily_log_wkf.to_csv(drive_path+f"/{today_str}_logistic_status_update.csv", index=False)


end = time.time()

with open(log_path, "a") as file:
    data = file.write(f'{date.today().strftime("%Y-%m-%d")} - TOOK {round((end - start)/60,2)} MINUTES - GET {len(daily_log_wkf[daily_log_wkf["date_check"]==today_str])} RESULTS\n')
