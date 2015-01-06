# Title: Adverse Impact_Race
# Client: Harte Hanks- NA
# Author: Chanel Chu
# Date: 2014-11-10

import datetime
import numpy as np
import pandas as p

class AdverseImpact_Race(object):
    
    def __init__(self, client_id):
        self.client_id = client_id
        
    # build query to be pulled from database
    def raw_data(self):
    
        raw_data = p.read_csv('C:/Users/Chanel Chu/Desktop/AI Harte Hanks/HH_race.csv')
        
        # set minimum of data points
        if len(raw_data) < 250:
            print 'Not enough observations available'   
                   
        else:    
            # standardize the variables
            raw_data['race'] = raw_data.race.str.upper()
            
            Race = {
                'HAWAIIAN':'HAWAIIAN OR PACIFIC ISLANDER',
                'PACIFIC ISLANDER':'HAWAIIAN OR PACIFIC ISLANDER',
                'BLACK':'BLACK OR AFRICAN AMERICAN',
                'AFRICAN':'BLACK OR AFRICAN AMERICAN',
                'AMERICAN INDIAN OR ALASKA NATIVE':'AMERICAN INDIAN OR ALASKAN',
                'AMERICAN INDIAN':'AMERICAN INDIAN OR ALASKAN',
                'ALASKAN':'AMERICAN INDIAN OR ALASKAN',
                'TWO OR MORE RACES':'TWO OR MORE',
                'I DO NOT WISH TO PROVIDE THIS INFORMATION':'NOT SPECIFIED',
                'WHITE':'WHITE OR CAUCASIAN',
                'INDIAN': 'ASIAN OR INDIAN',
                'ASIAN':'ASIAN OR INDIAN',
                'HISPANIC': 'HISPANIC OR LATINO',
                'LATINO': 'HISPANIC OR LATINO'
                }
            
            raw_data = raw_data.replace({'race': Race})
            return raw_data    

    # create dictionary of sample sizes 
    def sample_size(self, raw_data):
        
        # give values to scores (GY = 1 for passing, R = 0 for failing)
        score_value = {
                    'GREEN' : 1,
                    'YELLOW' : 1,
                    'RED' : 0         
                    }
        raw_data = raw_data.replace(score_value)
        
        # filtering raw_data by race to get count 
        # sample dictionary
        race = ['HAWAIIAN OR PACIFIC ISLANDER',
                  'BLACK OR AFRICAN AMERICAN',
                  'AMERICAN INDIAN OR ALASKAN',
                  'WHITE OR CAUCASIAN',
                  'ASIAN OR INDIAN',
                  'HISPANIC OR LATINO',
                  'NOT SPECIFIED',
                  'TWO OR MORE',
                  'UNKNOWN']
        sample = {}  
        for x in race:
            sample[x] = len(raw_data[raw_data['race'] == x])
        raw_data['id'] = raw_data['race'].map(sample)

        return raw_data 
    
    # create new table that has gender/race group x count of pass, count of fail, count of total
    # calculate pass rate for each group [pass rate = GY/(total GYR)]
    def pass_rate(self, raw_data):

        groups = raw_data.groupby(by = ['race'],).agg([np.mean])
        passrate = groups.reset_index()
        passrate.columns = passrate.columns.droplevel(1)    
        passrate.rename(columns={'id':'N'}, inplace= True)
        passrate = passrate[passrate.race != 'NOT SPECIFIED'] 
        return passrate
    
    # create new variable that is the max of each column multiplied by 0.8
    def threshold(self, passrate):
                
        # setting minimum limit for each race group to be greater than 2% of total N
        df = passrate[passrate.N > passrate['N'].sum()*0.02]  
            
        colnames = list(passrate.columns.values)
        del colnames[0:2]
        for i in colnames:
            passrate['score_impact_%s' % i] = df[i]/float(df[i].max())   
            
        colnames1 = list(passrate.columns.values)
        del colnames1[0:2]
        del colnames1[3:]
        for i in colnames1:
            passrate['N_GREEN_%s' % i] = 'blank'
            passrate['N_YELLOW_%s' % i] = 'blank'
            passrate['N_RED_%s' % i] = 'blank'    
               
        for i in colnames:
            passrate.rename(columns = {i:'passrate_%s' % i}, inplace = True)  
 
        passrate = passrate.reset_index()
        passrate = passrate.drop('index', 1)
        passrate = passrate.stack()
        print passrate
        return passrate

    def order(self):
        data = self.raw_data()
        data2 = self.sample_size(data)
        data3 = self.pass_rate(data2)
        data4 = self.threshold(data3)
        return data4    

if __name__ == "__main__":
    
    HHAI = AdverseImpact_Race(45)
    db = HHAI.order()
    db.to_csv('C:/Users/Chanel Chu/Desktop/AI Harte Hanks/AI_HH_Race_v2.csv')
