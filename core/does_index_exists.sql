


/*
FUNCTION: does_index_exists(text, text)
	description : 
	checks if a unique index already exists on form_data ->> '__id'
	
	parameters :
	schemaname text 		-- the name of the schema
	tablename text		-- the name of the table
	
	returning :
	boolean
*/

CREATE OR REPLACE FUNCTION odk_central.does_index_exists(
	schemaname text, 
	tablename text)
	RETURNS boolean AS 
	$BODY$
		SELECT count(indexname)>0 AS nb_indexes
		FROM pg_indexes
		WHERE schemaname = $1 and tablename = $2
		AND (indexdef ILIKE '%form_data ->> ''__id''%' OR indexdef ILIKE '%USING btree (data_id)%')
	$BODY$ 
LANGUAGE SQL;

COMMENT ON function does_index_exists(text,text) IS 'description : 
	checks if a unique index already exists on form_data ->> ''__id''
	
	parameters :
	schemaname text 		-- the name of the schema
	tablename text		-- the name of the table
	
	returning :
	boolean';