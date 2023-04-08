import spacy
import pandas as pd
import os
from google.cloud import storage
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credential.json"

client= storage.Client()
export_bucket = client.get_bucket('the-movie-database-project')

jobs_old=pd.read_csv('gs://the-movie-database-project/upwork/jobs.csv',index_col=0,sep='|')

NER = spacy.load("en_core_web_sm")


for i in range(len(jobs_old)):
    if jobs_old.loc[i,'language']=='es':
        next
    else:
        text=jobs_old.loc[i,'entry']
        regexp = r"(?= Posted On: )"
        text_final = re.split(regexp, text)[0]    
        text_entities= NER(text_final)
        entity_vector_inicial=[[word.text,word.label_] for word in text_entities.ents]
        unique_list = list(set(tuple(inner_list) for inner_list in entity_vector_inicial))
        entity_vector = [list(t) for t in unique_list]
        jobs_old.loc[i,'entity_vector']=str(entity_vector)


export_bucket.blob('upwork/jobs.csv').upload_from_string(jobs_old.to_csv(sep='|'),'text/csv')



#4- eliminar este caracter • desde antes (agregar .replace("•",""))
