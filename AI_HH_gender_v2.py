# Title: Adverse Impact_Gender
# Client: Harte Hanks- NA
# Author: Chanel Chu
# Date: 2014-11-10

import datetime
import numpy as np
import pandas as p

class AdverseImpact_Gender(object):
    
    def __init__(self, client_id):
        self.client_id = client_id

    # build query to be pulled from database
    def raw_data(self):
    
        raw_data = p.read_csv('C:/Users/Chanel Chu/Desktop/AI Harte Hanks/HH_Gender.csv')
        # print raw_data 
        
        # set minimum of data points
        if len(raw_data) < 250:
            print 'Not enough observations available' 
                     
        else:    
            # standardize the variables
            raw_data['gender'] = raw_data.gender.str.upper()
                
            Gender = {
                    'Female':'FEMALE', 
                    'female':'FEMALE', 
                    'FMALE':'FEMALE', 
                    'f':'FEMALE',  
                    'F':'FEMALE', 
                    'Male':'MALE', 
                    'male':'MALE',
                    'm':'MALE', 
                    'M':'MALE'
                    }
            
            raw_data = raw_data.replace({'gender': Gender})
            raw_data = raw_data.rename(columns={'JFF_Technical Support_Clr': 'JFF_TechnicalSupport_Clr'})

            return raw_data 
       
    # create new table that has gender/race group x count of pass, count of fail, count of total
    # calculate pass rate for each group [pass rate = GY/(total GYR)]
    def pass_rate(self, raw_data):
        score_value = {
                'GREEN' : 1,
                'YELLOW' : 1,
                'RED' : 0         
                } 
        scored =  raw_data.replace(score_value)
    
        gender = ['FEMALE','MALE','NOT SPECIFIED']
        sample1 = {}
        
        for x in gender:
            sample1[x] = len(raw_data[raw_data['gender'] == x])

        scored['id'] = raw_data['gender'].map(sample1)
                
        groups = scored.groupby(by = ['gender'],).agg([np.mean])
        passrate = groups.reset_index()
        passrate.columns = passrate.columns.droplevel(1)    
        passrate.rename(columns={'id':'N'}, inplace= True)  
        passrate = passrate[passrate.gender != 'NOT SPECIFIED']



        ###############################################
        

        gender = ['FEMALE','MALE','NOT SPECIFIED']
        score = ['GREEN', 'YELLOW', 'RED']
        sample2 = {}
                
        colnames = list(raw_data.columns.values)
        del colnames[0:2]
        
        for z in gender:
            for y in colnames:     
                for x in score:
                    sample2[z,y,x] = len(raw_data[(raw_data['%s' % y] == x) & (raw_data['gender'] == z)])

        df = p.DataFrame(sample2.items(), columns=['gender', 'N'])   
        df['gender'] = df['gender'].apply(str).str.replace("\(|\)","")
        df['gender'] = df['gender'].apply(str).str.replace("'", "")
        
        N = df['N']
        df.drop(labels=['N'], axis=1,inplace = True)
        
        df['jff'] = df['gender'].apply(lambda x: x.split (',')[1])
        df['color'] = df['gender'].apply(lambda x: x.split (',')[2])
        df.gender = df['gender'].apply(lambda x: x.split (',')[0])
        
        
        df['jff_color'] = df['jff']+'_'+df['color']+'_'+'N'
        df['jff_color'] = df['jff_color'].apply(str).str.replace(" ", "")
        
        df.drop(labels=['jff'], axis=1,inplace = True)
        df.drop(labels=['color'], axis=1,inplace = True)
        df.insert(2, 'N', N)

        df = df.pivot('gender', 'jff_color', 'N')
        df = df.reset_index()

        ###############################################


        passrate = p.merge(passrate,df, on = 'gender', how = 'outer') 
        passrate = passrate[passrate.gender != 'NOT SPECIFIED']

        return passrate        
    
    # create new variable that is the max of each column multiplied by 0.8
    def threshold(self, passrate):

        colnames = list(passrate.columns.values)
        del colnames[0:2]
        del colnames[3:]
        
        # setting minimum limit for each gender group to be greater than 2% of total N
        df = passrate[passrate.N > passrate['N'].sum()*0.02]
        
        for i in colnames:
            passrate['score_impact_%s' % i] = df[i]/float(df[i].max())   
            
        for i in colnames:
            passrate.rename(columns = {i:'passrate_%s' % i}, inplace = True)
        
        passrate = passrate.stack()
        passrate = passrate.unstack(level = 0)
        return passrate

    def order(self):
        data2 = self.raw_data()
        data3 = self.pass_rate(data2)
        data4 = self.threshold(data3)
        print data4
        return data4 
    
if __name__ == "__main__":
    
    HHAI = AdverseImpact_Gender(45)
    db = HHAI.order()
    db.to_csv('C:/Users/Chanel Chu/Desktop/AI Harte Hanks/AI_HH_Gender_v2.csv')
