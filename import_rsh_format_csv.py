import csv
import json
import pandas as pdexcel
import requests
import csv
import time
import os
from datetime import datetime

uid_dataset = [{'tRM_CSB_Cons_Ext': 'GSUZkSLVfZy'}, {'tRM_CSB_Violences_traumatismes':'GSUZkSLVfZy'}, {'tRM_CSB_CEXT_Utilisation_Prescription': 'GSUZkSLVfZy'},
               {'tRM_CSB_Depistage_PEC': 'ToV2hqPjApV'}, {'tRM_CSB_Depistage_PEC_ISTVIH': 'ToV2hqPjApV'},
               {'tRM_CSB_SURVEILLANCE_NUT': 'fVhjtnQnFpu'}, {'tRM_CSB_CONS_PRENAT': 'zVl3gPlL9HO'},
               {'tRM_CSB_Maternite': 'zVl3gPlL9HO'}, {'tRM_CSB_PEV_Enfants': 'zVl3gPlL9HO'},
               {'tRM_CSB_PF': 'zVl3gPlL9HO'}, {'tRM_CSB_DENTISTERI': 'kuBlaKJhV7W'},
               {'tRM_CSB_Scolaire': 'QDJ43MNqsGP'}, {'tRM_CSB_GES_STO_INTRANTS': 'NZ52X9Fw4lB'},
               {'tRM_CSB_INTRANT_IST_VIH_NUT_PF': 'NZ52X9Fw4lB'}, {'tRM_CSB_INTRANT_Tub_Lepre_MSR': 'NZ52X9Fw4lB'},
               {'tRM_CSB_FANOME': 'NZ52X9Fw4lB'}, {'tRM_CSB_GESTFIN': 'fYvqcl44zlF'}]


def browse_json_array(label, mylist):
    result = ''
    for value in mylist:
        try:
            result = value[label]
        except:
            pass
    return result


def get_uid_ou_dhis2(vcodegesis):
    uiddhis2 = ""

    csv_file = open('metadatagesis/fscsv.csv', 'r')
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        codegesis = line['CODE_GESIS']
        codegesis = int(codegesis)
        # print("codegesis="+codegesis)
        if codegesis == vcodegesis:
            uiddhis2 = line['UID_DHIS2']
            print("uiddhis2=" + uiddhis2)

    return uiddhis2


def get_de_uid(vcolonnedatarsh):
    de_coc_uid = ""
    de_uid = ""

    csv_file = open('metadatarsh/dershcsv.csv', 'r')
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        colonnefilemapping = str(line['columnCodeRSH'])
        if colonnefilemapping == vcolonnegesis:
                de_uid = line['DataElementUId']
                coc_uid = line['CoCUid']

    print("colonne datarsh=" + vcolonnedatarsh + "de_uid=" + de_uid)
    return de_uid


def get_co_uid(vcolonnedatarsh):
    coc_uid = ""
    csv_file = open('metadatarsh/dershcsv.csv', 'r')
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        colonnefilemapping = line['columnCodeRSH']
        if colonnefilemapping == vcolonnedatarsh:
            de_uid = line['DataElementUId']
            coc_uid = line['CoCUid']

    return coc_uid


def check_if_row_dataframe_has_value(df, index, colonneinutile, codegesisfs, uidoudhis2):
    res = False
    if (uidoudhis2 != ""):
        i = 0
        for col_name in df.columns:
            if pdexcel.isnull(dfexcel.at[index, col_name]) == False and col_name not in colonneinutile:
                i += 1
        if i > 0:
            res = True
        else:
            print("Existance uid dhis2 introuvable pour le code gesis", codegesisfs)

    return res


def get_periode_format_annee_mois(vAnnee, vMois):
    zAnneeMois = ""
    zMois = str(vMois)
    zAnnee = str(vAnnee)
    if len(zMois) == 1:
        zMois = "0" + zMois
    zAnneeMois = zAnnee + zMois
    return zAnneeMois


def importer_gesis_vers_dhis2(filedbname, ListNomColonneInutile):
    global dfexcel
    
    dfexcel = pdexcel.read_csv(filedbname)
    #dataSetID = browse_json_array(tablename, uid_dataset)
    dataSetID = "W5F89C7pATi"
    iNombreLigne = 0
    for index, row in dfexcel.iterrows():
        # apres 520079e ligne
        # if(639357+24 == 639381, 21014 à 10 09 2021 à 08h41):
        if (index > 0):
            if iNombreLigne != 5000:
                codefsrsh = int(dfexcel.loc[index, 'code_FS'])
                ou_uid = get_uid_ou_dhis2(codefsrsh)
                if check_if_row_dataframe_has_value(dfexcel, index, ListNomColonneInutile, codefsrsh, ou_uid) == True:
                    print("\n\n")
                    print("debut Iteration ligne")
                    zPeriode = "2022w52"
                    print("periode=" + zPeriode)
                    for col_name in dfexcel.columns:

                        # print("nomcolonne=",col_name,"valeur colonne=",value_colonne)
                        # if dfexcel.loc[index, col_name] != "nan" and col_name not in ListNomColonneInutile:
                        if pdexcel.isnull(
                                dfexcel.at[index, col_name]) == False and col_name not in ListNomColonneInutile:
                            print("code gesis fs=", codefsrsh)
                            print("ou =" + ou_uid)
                            print("periode=", zPeriode)
                            print("ds =" + dataSetID)
                            value_colonne = int(dfexcel.loc[index, col_name])
                            print("nomcolonne=", col_name, "valeur colonne=", value_colonne)
                            de_uid = get_de_uid(tablename, col_name)
                            coc_uid = get_co_uid(tablename, col_name)
                            print("de_uid=", de_uid)
                            print("co_uid=", coc_uid)
                            print("fin colonne")
                            print("\n")
                            if (de_uid != "" and len(de_uid) == 11 and coc_uid != "" and len(coc_uid) == 11):
                                submit(de_uid, coc_uid, dataSetID, ou_uid, zPeriode, str(value_colonne))
                                #time.sleep(0.00125)
                                time.sleep(0.00125)

                        else:
                            continue
                    print("fin iteration colonne")
                    #terminerdataset(dataSetID, zPeriode, ou_uid)
                    iNombreLigne += 1
                    print("iNombre ligne =" + str(iNombreLigne))
                    # time.sleep(0.00125)
                    time.sleep(0.00125)

                else:
                    continue
            else:
                time.sleep(60)
                iNombreLigne = 0
                print("Pause 30 seconde iNombreLigne=" + str(iNombreLigne))
            print("fin insertion ligne N°", index, "du code gesis", codegesisfs, "periode", zPeriode)
            print("================================================================================")
            print("================================================================================")
            print("fin insertion ligne N°", index, "du code gesis", codegesisfs, "periode", zPeriode)
            print("================================================================================")
            print("================================================================================")
            print("fin insertion ligne N°", index, "du code gesis", codegesisfs, "periode", zPeriode)
            print("================================================================================")
            print("================================================================================")
            print("================================================================================")


        else:
            continue

    print("============fin parcours ligne====================")


def submit(de, co, ds, ou, pe, value):
    # payload = {}
    print("n\n")
    try:

        if de != "null":
            payload = {}
            url = "https://simr.snis-sante.net/api/dataValues"
            # url = "http://localhost:8080/api/dataValues"
            url_for_custom_form = "http://localhost:8080/api/dataValues?de=" + de + "&co=" + co + "&ds=" + ds + "&ou=" + ou + "&pe=" + pe + "&value=" + value
            post = requests.post(url_for_custom_form, auth=(os.environ["api_fetcher_auth"], os.environ["api_fetcher_password"]),
                                 data=payload)

            if post.status_code not in [200, 201]:
                print("status code:" + str(post.status_code) + "text sup " + post.text)
                raise ValueError("Data could not be submitted")
            else:
                print("data was submitted")


        else:
            url = "https://simr.snis-sante.net/api/completeDataSetRegistrations"
            # url = "http://127.0.0.1:8080/api/completeDataSetRegistrations"
            if value == "complete":
                completed = "true"
            else:
                completed = "false"
            my_json_data = {
                "completeDataSetRegistrations":
                    [
                        {
                            "dataSet": ds,
                            "period": pe,
                            "organisationUnit": ou,
                            "completed": completed
                        }
                    ]
            }
            post = requests.post(url, json=my_json_data, auth=(os.environ["api_fetcher_auth"], os.environ["api_fetcher_password"]))

            try:
                print("post result :" + post.json())
            except:
                pass

            if post.status_code not in [200, 201]:
                raise ValueError("Data could not be completed")
            else:
                print("data was completed")
    except Exception as e:
        raise ValueError("Error de compilation" + str(e))
    print("\n\n")


def terminerdataset(ds, pe, ou):
    value = ""
    url = "https://simr.snis-sante.net/api/completeDataSetRegistrations"
    # url = "http://127.0.0.1:8080/api/completeDataSetRegistrations"
    if value == "complete":
        completed = "true"
    else:
        completed = "false"
    completed = "true"
    my_json_data = {
        "completeDataSetRegistrations":
            [
                {
                    "dataSet": ds,
                    "period": pe,
                    "organisationUnit": ou,
                    "completed": completed
                }
            ]
    }
    post = requests.post(url, json=my_json_data, auth=(os.environ["api_fetcher_auth"], os.environ["api_fetcher_password"]))

    try:
        print("post result :" + post.json())
    except:
        pass

    if post.status_code not in [200, 201]:
        raise ValueError("Data could not be completed")
    else:
        print("data was completed")


# Press the green button in the gutter to run the script.
# ListNomColonneInutileTab3 = "cAnnee cCodeNiv cCodeStru cPeriode cTypeRapport cType cCode c$_Tot_M c$_Tot_F c$_Tot_NC"
ListNomColonneInutileRSH= "RSH_1 AUTRES_1 AUTRES_2 NUM_SENDER"
# filedbname = 'Tab3TestErreur1.csv'
filedbname = 'datatestrsh.csv'
#Tablename = 'tRM_CSB_FANOME'
# print(os.environ.get('w_param'))
importer_gesis_vers_dhis2(filedbname, ListNomColonneInutileRSH)