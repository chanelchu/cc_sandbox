# Author: Chanel Chu
# Date: 2014-10-17

import mysqldb as sql
import datetime

from dsol.connector.db import ConnectHirex

hirex_connection = ConnectHirex(env='prod')

hirex_cursor = hirex_connection.cursor()

# build query to be pulled from database
def provideQuery(client_id, limit=250):

# standardize the variables and values
# set minimum of data points


# calculate pass rate for each ethnic group
# pass rate = GY/(total GYR)
# create def variable pass_rate
def pass_rate(ethnic_group):
    pass_rate = (ethnic_group.numscore_g + ethnic_group.numscore_y) / float(ethnic_group.total_numscore)
    return pass_rate

# sort ethnic group with highest pass rate
sorted(ethnic_group, pass_rate, reverse = True)

# highest ethnic_group pass rate * 0.8
def pass_rate_threshold(ethnic_group):
    pass_rate_threshold = pass_rate * 0.8
    return pass_rate_threshold

# lowest ethnic_group pass rate can't be lower than pass_rate_threshold
def adverse_impact_results(ethnic_group, pass_rate, pass_rate_threshold):
    if ethnic_group.pass_rate >= pass_rate_threshold:
        return 1
    else:
        return 0


