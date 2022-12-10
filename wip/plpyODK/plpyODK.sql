/* Récupération des submissions_id filtrés */
/* sous la forme "Submissions('uuid%3Adf981ea2-8c28-4133-9bfe-06c02c75522e')" pour utilise rensuite une fonction générique */

CREATE OR REPLACE FUNCTION plpyodk.client_get_filtered_submissions_ids(
	param_project_id text,
	param_form_id text,
	filter text)
    RETURNS text[]
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	from pyodk.client import Client
	client = Client()
	client.open()
	response = client.get("projects/"+param_project_id+"/forms/"+param_form_id+".svc/Submissions?$filter=__system/"+filter)
	submissions = []
	for s in (response.json()['value']):
		submissions.append("Submissions('"+s['__id']+"')")
	return submissions
	
$BODY$;

/*
SELECT array_to_string(plpyodk.client_get_filtered_submissions_ids(
	'5',
	'Sicen_2022',
	'submissionDate ge 2022-12-01'),',');

retourne : Submissions('uuid:8700f252-bd7a-4373-909a-6eba210bd56c'),Submissions('uuid:df981ea2-8c28-4133-9bfe-06c02c75522e'),Submissions('uuid:f78250e7-ff0b-4537-ac54-0042759a27cc')
*/



CREATE OR REPLACE FUNCTION plpyodk.client_get_table_by_path(
	param_project_id text,
	param_form_id text,
	paths text)
    RETURNS json
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	from pyodk.client import Client
	import json
	client = Client()
	client.open()
	data = []
	paths_list = paths.split(",")
	for path in paths_list:
		response = client.get("projects/"+param_project_id+"/forms/"+param_form_id+".svc/"+path)
		data.append(response.json())

	return(json.dumps(data))

$BODY$;

SELECT plpyodk.client_get_table_by_path(
	'5',
	'Sicen_2022',
    (SELECT array_to_string(plpyodk.client_get_filtered_submissions_ids(
	'5',
	'Sicen_2022',
	'submissionDate ge 2022-12-01'),',')))
	