CREATE OR REPLACE FUNCTION plpyodk.get_filtered_datas(
	param_project_id text,
	param_form_id text,
	param_criteria text
)
    RETURNS text
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
def fresh_data_only(project_id, form_id, path, criteria,  datas):
    from pyodk.client import Client
    import re
    import json 
    client = Client()
    client.open()
    if path == '':
        return None

    url = 'projects/'+project_id+'/forms/'+form_id+'.svc/'+path+'?$filter='+criteria
    if re.match(r"Submissions\?.*", path) or re.match(r".*\)$", path):
        tablename = 'submissions'
    else:
        tablename = path.rsplit('/')[-1]    
    
    response = client.get(url)
    
    value = response.json()['value']
    
    navigationlinks = re.findall(r'(\'\w+@odata\.navigationLink\'):\s+([^\}]+)', str(value))
    for (key, link) in navigationlinks:
        link = link.replace("'", "'").replace('"','')
        fresh_data_only(project_id, form_id, link, '', datas)
    
    if tablename in datas.keys():
        datas[tablename] += value
    else:
        datas[tablename]=value
		
    json_datas = json.dumps(datas, indent = 4) 
    return json_datas
	
return fresh_data_only(param_project_id, param_form_id, 'Submissions', param_criteria, datas = {})

$BODY$;
/*
SELECT * from json_each(plpyodk.get_filtered_datas('5', 'Sicen_2022','__system/submissionDate ge 2023-03-05')::json)
*/