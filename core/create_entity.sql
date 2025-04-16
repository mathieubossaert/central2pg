CREATE OR REPLACE FUNCTION odk_central.create_entity(
	email text,
	password text,
	central_domain text,
	data_ json, 
	entity_list_name_ text default null::text, 
	project_id_ integer default null::integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id_,'/datasets/',entity_list_name_,'/entities');
EXECUTE (
		'DROP TABLE IF EXISTS central_text_from_central;
		 CREATE TEMP TABLE central_text_from_central(form_data text);'
		);
EXECUTE format('COPY central_text_from_central FROM PROGRAM $$ curl  --insecure --include --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' -H "Content-Type: application/json"  -X POST -d '''||data_||''' '||url||' $$ ;');

END;
$BODY$;

--> https://docs.getodk.org/central-api-entity-management/#creating-entities
--SELECT odk_central.create_entity('email','password','central.domain.org',	'{"label": "mare aux poules",  "data": { "surface": "15" }, "uuid": "f8976b20-773a-498c-98ea-6393c3581295"}'::json, 'mares_bidons', 18)

