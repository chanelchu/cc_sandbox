# Title: Adverse Impact
# Author: Chanel Chu
# Date: 2014-10-17

import MySQLdb as sql
import datetime
import numpy as np
import pandas as p
from dsol.connector.db import ConnectHirex

class AdverseImpact(object):
	
	def __init__(self, client_id, limit=60000):
		self.client_id = client_id
		self.limit = limit
		
	# build query to be pulled from database
	def sql_data(self):
		
		hirex_connection = ConnectHirex(env='prod')
		hirex_cursor = hirex_connection.cursor()
		
		base_query = '''
		SELECT
		s.id,
		a.gender_lkp_id AS gender,  
		a.race_lkp_id AS race,
		p.position_name as position,
		l.city as location,
		jfs.created_at AS scored_date,
		jfs.f10_string AS jff_technical_aptitude,
		jfs.f11_string AS jff_work_at_home_aptitude,
		jfs.f12_string AS jff_collections,
		jfs.f13_string AS jff_coach,
		jfs.f15_string AS jff_english_proficiency,
		jfs.f16_string AS jff_technical_support,
		jfs.f19_string AS jff_ts_interest,
		jfs.f1_string AS jff_overall_job_fit_factor,
		jfs.f21_string AS jff_ventas,
		jfs.f2_string AS jff_customer_service_level_1,
		jfs.f3_string AS jff_customer_service_level_2,
		jfs.f4_string AS jff_customer_service,
		jfs.f5_string AS jff_sales,
		jfs.f6_string AS jff_technical_support_level_1,
		jfs.f7_string AS jff_technical_support_level_2,
		jfs.f9_string AS jff_product_service,
		pfs.f13_string AS pf_service_orientation,
		pfs.f1_string AS pf_call_handling_efficiency,
		pfs.f3_string AS pf_attendance_and_dependability1,
		pfs.f45_string AS pf_comprehension_and_grammar,
		pfs.f46_string AS pfs_spelling_and_vocabulary,
		pfs.f52_string AS pfs_customer_handling_efficiency,
		pfs.f14_string AS pf_pursuing_goals_and_opportunities,
		pfs.f17_string AS pf_leading_and_managing_others,
		pfs.f18_string AS pf_listening_and_problem_resolution,
		pfs.f19_string AS pf_analyzing_and_interpreting_information,
		pfs.f1_numeric AS pf_interview_score,
		pfs.f20_string AS pf_communication,
		pfs.f29_string AS pf_technical_aptitude,
		pfs.f2_string AS pf_positive_call_resolution,
		pfs.f30_string AS pf_work_at_home_aptitude,
		pfs.f31_string AS pf_media_preference,
		pfs.f38_string AS pf_technical_knowledge_support,
		pfs.f39_string AS pf_productivity_and_efficiency,
		pfs.f3_string AS pf_attendance_and_dependability2,
		pfs.f47_string AS pf_ts_interest,
		pfs.f6_string AS pf_sales_proficiency
		
		FROM assessment s
		INNER JOIN applicant a on s.applicant_id = a.id
		INNER JOIN assessment_jfactor_score jfs on s.id = jfs.id
		INNER JOIN assessment_pfactor_score pfs ON s.id = pfs.id
		INNER JOIN position p on p.id = s.position_id
		INNER JOIN location l on l.id = s.location_id
		
		WHERE s.client_id = %d
		AND a.is_testing_flag = 0
		AND a.first_name NOT LIKE '%%TEST%%'
		AND a.first_name NOT LIKE '%%APPLICANT%%'
		AND a.last_name NOT LIKE '%%TEST%%'
		AND a.last_name NOT LIKE '%%APPLICANT%%'
		AND a.gender_lkp_id IS NOT NULL 
		AND a.race_lkp_id IS NOT NULL
		AND s.screening_status_lkp_id IS NOT NULL
		AND a.gender_lkp_id != "" 
		AND a.race_lkp_id != ""
		ORDER BY jfs.created_at DESC 
		''' % (self.client_id)
		
		if self.limit:
			limit_clause = 'LIMIT %d' % self.limit
		
		if limit_clause:
			base_query += (limit_clause)
	
		raw_data = p.read_sql(base_query,hirex_connection)
		# print raw_data 
		
		# set minimum of data points
		if len(raw_data) < 250:
			print 'Not enough observations available'		  
		else:	
			# standardize the variables
			raw_data['race'] = raw_data.race.str.upper()
			raw_data['gender'] = raw_data.gender.str.upper()
	
			# print raw_data 
			
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
		
			Race = {
				'HAWAIIAN':'HAWAIIAN OR PACIFIC ISLANDER',
				'PACIFIC ISLANDER':'HAWAIIAN OR PACIFIC ISLANDER',
				'BLACK':'BLACK OR AFRICAN AMERICAN',
				'AFRICAN':'BLACK OR AFRICAN AMERICAN',
				'AMERICAN INDIAN OR ALASKA NATIVE':'AMERICAN INDIAN OR ALASKAN',
				'AMERICAN INDIAN':'AMERICAN INDIAN OR ALASKAN',
				'ALASKAN':'AMERICAN INDIAN OR ALASKAN',
				'TWO OR MORE RACES':'TWO OR MORE',
				'UNKNOWN':'NOT SPECIFIED',
				'I DO NOT WISH TO PROVIDE THIS INFORMATION':'NOT SPECIFIED',
				'WHITE':'WHITE OR CAUCASIAN',
				'INDIAN': 'ASIAN OR INDIAN',
				'ASIAN':'ASIAN OR INDIAN',
				'HISPANIC': 'HISPANIC OR LATINO',
				'LATINO': 'HISPANIC OR LATINO'
				}
			
			raw_data = raw_data.replace({'gender': Gender})
			raw_data = raw_data.replace({'race': Race})
				
			# print raw_data
		
			# give values to scores (GY = 1 for passing, R = 0 for failing)
			score_value = {
					'GREEN' : 1,
					'YELLOW' : 1,
					'RED' : 0		 
					}
			raw_data = raw_data.replace(score_value)
			# print raw_data
			return raw_data	
	
	""" # create dictionary of sample sizes 
	def sample_size(self, raw_data):

		# sample dictionary
		gender = ['FEMALE','MALE']
		sample = {}
				
		# filtering raw_data by gender to get count 
		for x in gender:
			sample[x] = len(raw_data[raw_data['gender'] == x])

		raw_data['id'] = raw_data['gender'].map(sample)
		
		# sample dictionary
		race = ['HAWAIIAN OR PACIFIC ISLANDER',
				  'BLACK OR AFRICAN AMERICAN',
				  'AMERICAN INDIAN OR ALASKAN',
				  'WHITE OR CAUCASIAN',
				  'ASIAN OR INDIAN',
				  'HISPANIC OR LATINO',
				  'NOT SPECIFIED',
				  'TWO OR MORE']
		sample = {}
				
		# filtering raw_data by race to get count 
		for x in race:
			sample[x] = len(raw_data[raw_data['race'] == x])

		raw_data['id'] = raw_data['race'].map(sample)
		# print raw_data
		return raw_data """
	
	# create new table that has gender/race group x count of pass, count of fail, count of total
	# calculate pass rate for each group [pass rate = GY/(total GYR)]
	def pass_rate(self, raw_data):
		raw_data = raw_data.drop('id',1)
		groups = raw_data.groupby(by = ['gender','race'],).agg([np.mean])
		passrate = groups.reset_index()
		passrate.columns = passrate.columns.droplevel(1)	
		return passrate
	
	# create new variable that is the max of each column multiplied by 0.8
	def threshold(self, passrate):

		colnames = list(passrate.columns.values)
		del colnames[0:2]
		
		for i in colnames:
			passrate['max_%s' % i] = (max(passrate[i]))
			passrate['max_%s' % i] = passrate['max_%s' % i]
			passrate['max_%s' % i] = passrate['max_%s' % i]*.8
		
		for i in colnames:
			passrate['results_%s' % i] = ''
			passrate['results_%s' % i][passrate[i] >= passrate['max_%s' % i]] = 1
			passrate['results_%s' % i][passrate[i] < passrate['max_%s' % i]] = 0
			
		return passrate

	def order(self):
		data = self.sql_data()
		data2 = self.sample_size(data)
		data3 = self.pass_rate(data2)
		data4 = self.threshold(data3)
		
		return data4
		

if __name__ == "__main__":
	# hirex_connection = ConnectHirex(env='prod')
	# hirex_cursor = hirex_connection.cursor()
	
	NovoAI = AdverseImpact(41)
	db = NovoAI.order()
	db.to_csv('C:/Users/Chanel Chu/Desktop/adverse_impact.csv')
