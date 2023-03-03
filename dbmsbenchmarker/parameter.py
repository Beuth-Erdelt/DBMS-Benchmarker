"""
    Classes for randomizing queries of the Python Package DBMS Benchmarker
    Copyright (C) 2020  Patrick Erdelt

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import random
import time
import pandas as pd
from dateutil.relativedelta import relativedelta
import logging


demoparameters = {
    'first': {
        'type': "integer",
        'range': [1,10]
    },
    'second': {
        'type': "float",
        'range': [1,10]
    },
    'third': {
        'type': "list",
        'range': ['a','b','c']
    },
    'forth': {
        'type': "date",
        'range': ["2008-01-01", "2009-01-01"]
    },
    'fifth': {
        'type': "dict",
        'range': {"AFRICA": ["MOROCCO", "ALGERIA", "MOZAMBIQUE", "ETHIOPIA", "KENYA"],
"AMERICA": ["CANADA", "UNITED STATES", "ARGENTINA", "PERU", "BRAZIL"],
"ASIA": ["INDIA", "JAPAN", "INDONESIA", "VIETNAM", "CHINA"],
"EUROPE": ["GERMANY", "ROMANIA", "UNITED KINGDOM", "FRANCE", "RUSSIA"],
"MIDDLE EAST": ["IRAQ", "EGYPT", "SAUDI ARABIA", "JORDAN", "IRAN"]}
    }
}

defaultParameters = {}

# example usage:
# defaultParameters = {'SF': 10}

def generateParameters(parameters, number):
    logger = logging.getLogger('parameter')
    result = []
    for i in range(0, number):
        #print(k)
        result.append({})
        for k, v in parameters.items():
            if not 'size' in v:
                v['size'] = 1
            #print(i)
            w = randomizer(v).value
            #print(w)
            if type(w) == list:
                for j, value in enumerate(w):
                    result[i][k+str(j+1)] = value
            else:
                result[i][k] = w
        #result[i] = tools.joinDicts(result[i], defaultParameters)
    logger.debug(result)
    return result

# generateParameters(demoparameters,1)

class randomizer():
    def __init__(self, parameter):
        logger = logging.getLogger('parameter')
        f = getattr(self, parameter['type'])
        self.value = f(parameter)
        logger.debug(parameter)
    def integer(self, parameter):
        l = parameter['range']
        size = parameter['size']
        l2 = list(range(l[0],l[1]+1))
        resultlist = random.sample(l2,size)
        if size == 1:
            value = resultlist[0]
        else:
            value = resultlist
        return value
        #return random.randrange(l[0],l[1]+1)
    def float(self, parameter):
        l = parameter['range']
        return random.uniform(l[0],l[1])
    def list(self, parameter):
        l = parameter['range']
        size = parameter['size']
        resultlist = random.sample(l,size)
        if size == 1:
            value = resultlist[0]
        else:
            value = resultlist
        return value
    def date(self, parameter):
        # inspired by
        # # https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
        l = parameter['range']
        dateformat = '%Y-%m-%d'
        stime = time.mktime(time.strptime(l[0], dateformat))
        etime = time.mktime(time.strptime(l[1], dateformat))
        ptime = stime + random.random() * (etime - stime)
        return time.strftime(dateformat, time.localtime(ptime))
    def firstofmonth(self, parameter):
        l = parameter['range']
        start = pd.Timestamp(l[0])
        end = pd.Timestamp(l[1])
        # complete months between
        diff = relativedelta(end,start).years * 12 + relativedelta(end,start).months
        m = random.randrange(0, diff+1)
        new = start+relativedelta(months=m)
        return new.strftime("%Y-%m-01")
    def firstofyear(self, parameter):
        l = parameter['range']
        year = l[0]+random.randrange(0, l[1]-l[0]+1)
        return str(year)+"-01-01"
    def hexcode(self, parameter):
        l = parameter['range']
        size = parameter['size']
        l2 = list(range(l[0],l[1]+1))
        resultlist = random.sample(l2,size)
        resultlist = [str(hex(i)).upper()[2:] for i in resultlist]
        if size == 1:
            value = resultlist[0]
        else:
            value = resultlist
        return value
    def dict(self, parameter):
        d = parameter['range']
        k = list(d.keys())[random.randrange(len(d.keys()))]
        k2 = random.randrange(len(d[k]))
        v = d[k][k2]
        return [k, v]
