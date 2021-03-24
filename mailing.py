from collections import defaultdict
import pandas as pd
import math
Dict_stoke_mail=defaultdict(list)
open_count_size={}
hard_bounce_size={}
Soft_Bounce_size={}

data = pd.read_csv("C:\\Users\\Toshiba\\Desktop\\DATA\\Desktop\\FD\\yelo\\All_mails.csv", sep=";", engine='python',encoding='utf-8')
data = data.drop_duplicates(subset=['Campaign Name','Email_ID'])#print(data) # (Unique) un user peut apparaitre plusieurs fois avec la mmm compagnie name (on les supprime donc)


#on stoke dans le dict les emails et les name company
mail_compaign = dict(data.groupby('Email_ID')["Campaign Name"].apply(list))
mail_compaign.pop('Email_ID', None) # le pop car dans les lignes du fichiers il existe des header donc pour ne pas avoir Campaign Name comme value on pop

# compter le lenght de la clé
for key in mail_compaign:
    longueur=len(mail_compaign[key])
    mail_compaign[key]=longueur
print(mail_compaign)

open_count = dict(data.groupby('Email_ID')["Open_Date"].apply(list))
open_count.pop('Email_ID', None)
for k, v in open_count.items(): # mettre dans la liste and avoid repetition
    new_list = []
    for item in v:
        if item not in new_list:
            new_list.append(item)
    open_count[k] = new_list

for key in open_count:
    cpt=str(open_count[key]).count('nan')


    if cpt==len(open_count[key]):
        open_count_size[key]=0
    if ('nan' not in str(open_count[key])):
        open_count_size[key]=len(open_count[key])
    else:
        cpt = str(open_count[key]).count('nan')

        open_count_size[key]=(len(open_count[key]))- cpt

#print(open_count_size)

hard_bounce = dict(data.groupby('Email_ID')["Hard_Bounce_Date"].apply(list))
hard_bounce.pop('Email_ID', None)

for k, v in hard_bounce.items(): # mettre dans la liste and avoid repetition
    new_list = []
    for item in v:
        if item not in new_list:
            new_list.append(item)
    hard_bounce[k] = new_list

for key in hard_bounce:
    cpt=str(hard_bounce[key]).count('nan')


    if cpt==len(hard_bounce[key]):
        hard_bounce_size[key]=0
    if ('nan' not in str(hard_bounce[key])):
        hard_bounce_size[key]=len(hard_bounce[key])
    else:
        cpt = str(hard_bounce[key]).count('nan')

        hard_bounce_size[key]=(len(hard_bounce[key]))- cpt

Soft_Bounce = dict(data.groupby('Email_ID')["Soft_Bounce_Date"].apply(list))
Soft_Bounce.pop('Email_ID', None)

for k, v in Soft_Bounce.items(): # mettre dans la liste and avoid repetition
    new_list = []
    for item in v:
        if item not in new_list:
            new_list.append(item)
    Soft_Bounce[k] = new_list

for key in Soft_Bounce:
    cpt=str(Soft_Bounce[key]).count('nan')


    if cpt==len(Soft_Bounce[key]):
        Soft_Bounce_size[key]=0
    if ('nan' not in str(Soft_Bounce[key])):
        Soft_Bounce_size[key]=len(Soft_Bounce[key])
    else:
        cpt = str(Soft_Bounce[key]).count('nan')

        Soft_Bounce_size[key]=(len(Soft_Bounce[key]))- cpt

#Pour mettre le résultat dans un fichier csv
temp = []
for idx in mail_compaign:

    temp.append([idx, mail_compaign[idx],open_count_size[idx],hard_bounce_size[idx],Soft_Bounce_size[idx]])

pd.DataFrame(temp, columns=['mail', 'nbr_com','open','hard','soft']).to_csv("C:\\Users\\Toshiba\\Desktop\\DATA\\Desktop\\FD\\yelo\\result.csv", sep=";")