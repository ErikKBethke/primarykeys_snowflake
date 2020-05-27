#!/usr/bin/env python
# coding: utf-8

import pandas as pd
### list of variables to add to for return function querying
dict_queries = {}

### add queries below here
# need: variable with string query
# need: add variable to dict_queries

### bamboohr
str_pk_bamboo_perms = """
SELECT (supervisor_id||report_id), COUNT(*), concat(supervisor_id, report_id)
FROM (

select
      supervisor_5th as SUPERVISOR,
      supervisor_5th_workemail as SUPERVISOR_WORKEMAIL,
      supervisor_5th_id as SUPERVISOR_ID,
      department as DEPARTMENT,
      null as TERRITORY,
      displayname as REPORT_NAME,
      workemail as REPORT_WORKEMAIL,
      id as REPORT_ID
    from bamboohr.public.employees
    where status = 'Active' and HIREDATE <= CURRENT_DATE and supervisor_5th is not null
    UNION
    select
      supervisor_4th as SUPERVISOR,
      supervisor_4th_workemail as SUPERVISOR_WORKEMAIL,
      supervisor_4th_id as SUPERVISOR_ID,
      department as DEPARTMENT,
      null as TERRITORY,
      displayname as REPORT_NAME,
      workemail as REPORT_WORKEMAIL,
      id as REPORT_ID
    from bamboohr.public.employees
    where status = 'Active' and HIREDATE <= CURRENT_DATE and supervisor_4th is not null
    UNION
    select
      supervisor_3rd as SUPERVISOR,
      supervisor_3rd_workemail as SUPERVISOR_WORKEMAIL,
      supervisor_3rd_id as SUPERVISOR_ID,
      department as DEPARTMENT,
      null as TERRITORY,
      displayname as REPORT_NAME,
      workemail as REPORT_WORKEMAIL,
      id as REPORT_ID
    from bamboohr.public.employees
    where status = 'Active' and HIREDATE <= CURRENT_DATE and supervisor_3rd is not null
    UNION
    select
      supervisor_2nd as SUPERVISOR,
      supervisor_2nd_workemail as SUPERVISOR_WORKEMAIL,
      supervisor_2nd_id as SUPERVISOR_ID,
      department as DEPARTMENT,
      null as TERRITORY,
      displayname as REPORT_NAME,
      workemail as REPORT_WORKEMAIL,
      id as REPORT_ID
    from bamboohr.public.employees
    where status = 'Active' and HIREDATE <= CURRENT_DATE and supervisor_2nd is not null
    UNION
    select
      supervisor as SUPERVISOR,
      supervisor_workemail as SUPERVISOR_WORKEMAIL,
      supervisor_id as SUPERVISOR_ID,
      department as DEPARTMENT,
      null as TERRITORY,
      displayname as REPORT_NAME,
      workemail as REPORT_WORKEMAIL,
      id as REPORT_ID
    from bamboohr.public.employees
    where status = 'Active' and HIREDATE <= CURRENT_DATE and SUPERVISOR is not null)
  GROUP BY 1, 3
  ORDER BY 2 DESC;
"""
dict_queries['str_pk_bamboo_perms'] = str_pk_bamboo_perms

str_pk_bamboo_5k = """
SELECT *
FROM bamboohr.public.employees
LIMIT 5000;
"""
dict_queries['str_pk_bamboo_5k'] = str_pk_bamboo_5k

str_pk_bamboo_emp = """
SELECT id, COUNT(*)
FROM bamboohr.public.employees
GROUP BY id
ORDER BY 2
LIMIT 5;
"""
dict_queries['str_pk_bamboo_emp'] = str_pk_bamboo_emp

### highspot
str_pk_highspot = """
SELECT *
FROM load_prod.highspot.reports
WHERE IFNULL(user_id, 'abc')||IFNULL(content_id, 'abc') = 'abc5d5aef81628ba273c30e0681';
"""
dict_queries['str_pk_highspot'] = str_pk_highspot

str_highspot_5k = """
SELECT *
FROM load_prod.highspot.reports
LIMIT 5000;
"""
dict_queries['str_highspot_5k'] = str_highspot_5k

def return_vars():
    """
    Returns a dictionary of all the query:string pairs within the query file.

    Parameters
    ----------
    None

    Returns
    -------
    Dictionary
        Dictionary of all query:string pairs.
    """
    return dict_queries