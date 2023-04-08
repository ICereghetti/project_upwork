import pandas as pd
import feedparser
from guess_language import guess_language
import re
from bs4 import BeautifulSoup
from datetime import datetime,time
from datetime import timedelta
import numpy as np
from tqdm import tqdm
import os
from google.cloud import storage
import fsspec
import gcsfs
import smtplib
from email.mime.text import MIMEText
from ast import literal_eval

def upwork():

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credential.json"
    
    nz_time=timedelta(hours = 12)
    
    #### LEO 2 ARCHIVOS EN GC 

    client= storage.Client()
    export_bucket = client.get_bucket('the-movie-database-project')


    jobs_old=pd.read_csv('gs://the-movie-database-project/upwork/jobs.csv',index_col=0,sep='|')
    

    skills_validadas=pd.read_csv('gs://the-movie-database-project/upwork/skills_validated.csv')
        
    feed_link='https://www.upwork.com/ab/feed/jobs/rss?q=%28NOT+%28Click+at+Job+description+Section%29%29+AND+title%3A%28Data+Analyst%29&sort=recency&proposals=0-4%2C5-9&paging=0%3B50&api_params=1&securityToken=bf14e9102c13a988c86313f0eeca7704349d8d44a2843e7b69784ce82b56c6776d3ec0a1de1e6f2db887007a777d3f82d3630e14d30e7585a9c572fda5966e25&userUid=1200755288052822016&orgUid=1200755288065404929'
        
    Feed = feedparser.parse(feed_link)
    
    jobs_new=pd.DataFrame(columns=['datetime_published','title','entry','datetime_execution','link'])
    
    for i in tqdm(range(0,len(Feed.entries))):
        title = Feed.entries[i]['title']
        title = title.replace("|", "-").replace("\n",'. ').replace("\t",'. ')
        entry = Feed.entries[i]['summary']
        entry = entry.replace("|", "-")
        link = Feed.entries[i]['link']
        datetime_published_utc=datetime(Feed.entries[i]['published_parsed'][0], Feed.entries[i]['published_parsed'][1], Feed.entries[i]['published_parsed'][2], Feed.entries[i]['published_parsed'][3], Feed.entries[i]['published_parsed'][4], Feed.entries[i]['published_parsed'][5], 0)
        datetime_published=datetime_published_utc+nz_time
        datetime_execution=datetime.utcnow()+nz_time
        data=pd.DataFrame([[datetime_published,title,entry,datetime_execution,link]],columns=['datetime_published','title','entry','datetime_execution','link'])
        jobs_new=pd.concat([jobs_new,data])
    
    jobs_new.datetime_published=jobs_new.datetime_published.astype(str)
    jobs_old.datetime_published=jobs_old.datetime_published.astype(str)
    
    m = jobs_new.reset_index().merge(jobs_old, on=['datetime_published','title'], how='left', 
                                     suffixes=['', '_'], indicator=True)
    
    
    new_values=m[m['_merge']=='left_only'].index
    
    jobs_new=jobs_new.reset_index(drop=True).loc[new_values]
    
    jobs_new=jobs_new.reset_index(drop=True)
    
    exclusion_list=['Los Angeles based real estate company seeking Full Time Statistician']
    

    for i in exclusion_list:
        jobs_new['entry'].str.contains(i)
        jobs_new=jobs_new[~jobs_new['entry'].str.contains(i)]

    jobs_new=jobs_new.reset_index(drop=True)
    
    
    jobs=pd.DataFrame(columns=['datetime_published','score','title','entry','language','category','skills','entity_vector','datetime_execution','link'])
    
    if len(jobs_new)>0:
    
        for i in tqdm(range(0,len(jobs_new))):
            datetime_published=jobs_new.loc[i,'datetime_published']
            datetime_execution=jobs_new.loc[i,'datetime_execution']
            title=jobs_new.loc[i,'title']    
            link=jobs_new.loc[i,'link']    
            entry = jobs_new.loc[i,'entry'] 
            lan=guess_language(entry)
            soup = BeautifulSoup(entry)
            text=soup.get_text()
            a=re.search(r'(?<=Category: )(.*)',text).group()
            category=re.search(r'(.+)(?=Skills)',a).group()
            c=re.search(r'(?<=Skills:)(.+)',a).group()
            d=c.split(',')
            skills=[x.strip() for x in d]
            entity_vector=np.nan       
            skills_case_array=np.array(skills)
            skills_all=np.array(skills_validadas.skill)
            skills_validadas_encv=np.array(skills_validadas[skills_validadas['validation']==1].skill)
            skills_no_validadas_encv=np.array(skills_validadas[skills_validadas['validation']==0].skill)
        
            skills_ok=np.intersect1d(skills_case_array,skills_validadas_encv)
            skills_neutral=np.intersect1d(skills_case_array,skills_no_validadas_encv)
            skills_bad=np.setdiff1d(skills_case_array, skills_all)
            score=round((len(skills_ok)*1+len(skills_neutral)*0+len(skills_bad)*-1)/len(skills_case_array)*100)
            text=text.replace("\n",'. ').replace("\t",'. ')
            data=pd.DataFrame([[datetime_published,score,title,text,lan,category,skills,entity_vector,datetime_execution,link]],columns=['datetime_published','score','title','entry','language','category','skills','entity_vector','datetime_execution','link'])
            jobs=pd.concat([jobs,data])    

        
        jobs=jobs.reset_index(drop=True)
        
        jobs_final=pd.concat([jobs_old,jobs])

        jobs_final=jobs_final.reset_index(drop=True)


        ## REESCRIVO ARCHIVO EN GC
        
        client= storage.Client()

        export_bucket = client.get_bucket('the-movie-database-project')


        export_bucket.blob('upwork/jobs.csv').upload_from_string(jobs_final.to_csv(sep='|'),'text/csv')

        

        def is_time_between(begin_time, end_time):
            # If check time is not given, default to current UTC time
            check_datetime = datetime.utcnow()+nz_time
            check_time=check_datetime.time()
            if begin_time < end_time:
                return check_time >= begin_time and check_time <= end_time
            else: # crosses midnight
                return check_time >= begin_time or check_time <= end_time
        
        # Original test case from OP
        
        
        ## Preparo envio de mail
        
        if is_time_between(time(6,58), time(23,5)):        
            mail_proposals=jobs[['score','link','title']].sort_values(by=['score'],ascending=False).head()
        
        
            a=mail_proposals.to_csv(header=None,index=False,sep=' ')
        
        
        
            #### MANDO MAIL CON MEJORES
                    
            def send_email(subject, body, sender, recipients, password):
                msg = MIMEText(body)
                msg['Subject'] = subject
                msg['From'] = sender
                msg['To'] = ', '.join(recipients)
                smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
                smtp_server.login(sender, password)
                smtp_server.sendmail(sender, recipients, msg.as_string())
                smtp_server.quit()
            
            # Open the credentials file and load its contents into a dictionary

# Open the JSON file and read its contents
            with open('credential_mail.json', 'r') as f:
                json_str = f.read()
            
            # Parse the JSON string into a Python object
            credentials = eval(json_str)

            # Access the username and password from the dictionary 
            subject = "New Upwork Offer"
            body = a
            sender = credentials['mail']
            recipients = [credentials['mail']]
            password = credentials['password']
            
            send_email(subject, body, sender, recipients, password)
        
        
        skills=pd.DataFrame()


        jobs_final=pd.read_csv('gs://the-movie-database-project/upwork/jobs.csv',sep='|')

        for i in range(len(jobs_final)):
            a=jobs_final.skills[i]
            b=list(literal_eval(a))
            c=pd.get_dummies(b)
            d=pd.DataFrame(c.sum(),columns=[i])
            e=d.T
            skills=pd.concat([skills,e])
            
        skills=skills.fillna(0)

        skills_total=pd.DataFrame(skills.sum(),columns=['total']).nlargest(10,'total')

        correlation=skills[skills_total.index].corr()

        a=correlation.reset_index()


        b=pd.melt(a, id_vars='index', value_vars=a.columns.drop('index'), value_name='v2')
            


        c=b.rename(columns={'index':'variable','variable':'index'})

        d=c.reset_index()

        d=b.merge(c.reset_index(), on=['index','variable'], how='left')


        for i in range(len(d)):
            try:
                value=d.loc[i,'level_0']
                d=d[d.index!=value]
            except KeyError:
                next
            
        d=d[['index','variable','v2_x']]

        skills_corr=d.rename(columns={'index':'skill_1','variable':'skill_2','v2_x':'value'})

                ## REESCRIVO ARCHIVO EN GC
                

        export_bucket.blob('upwork/skills_corr.csv').upload_from_string(skills_corr.to_csv(index=False,sep='|'),'text/csv')


        
        
    return print('Funcion terminada')

upwork()
    