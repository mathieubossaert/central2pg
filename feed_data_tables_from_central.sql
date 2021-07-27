/*
FUNCTION: feed_data_tables_from_central(text, text)

!!! You need to edit and set the correct search_path at the beginig of the EXECUTE statement : SET search_path=odk_central; Replace "odk_central" by the name of the schema where you created the functions !!!

	description : 
		Feed the tables from key/pair tables. 
	parameters :
		schema_name text	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text		-- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void

*/

CREATE OR REPLACE FUNCTION feed_data_tables_from_central(
	schema_name text,	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
	table_name text,	-- the table containing plain json submission from the get_submission_from_central() function call
	geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like 'point,polygon'... depending on your question names ex. : 'geopoint_widget_placementmap,point,ligne,polygone'
    )
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
--declare keys_to_ignore text;
BEGIN

RAISE INFO 'entering feed_data_tables_from_central for table %', table_name; 
EXECUTE format('SET search_path=odk_central,pubic;
	DROP TABLE IF EXISTS data_table;
	CREATE TABLE data_table(data_id text, key text, value json);
	INSERT INTO  data_table(data_id, key, value) 
	WITH RECURSIVE doc_key_and_value_recursive(data_id, key, value) AS (
	  SELECT
		('||table_name||'.form_data ->> ''__id'') AS data_id,
		t.key,
		t.value
	  FROM '||schema_name||'.'||table_name||', json_each('||table_name||'.form_data) AS t
	  UNION ALL
	  SELECT
		doc_key_and_value_recursive.data_id,
		t.key,
		t.value
	  FROM doc_key_and_value_recursive,
		json_each(CASE 
		  WHEN json_typeof(doc_key_and_value_recursive.value) <> ''object'' OR key = ANY(string_to_array('''||geojson_columns||''','','')) THEN ''{}'' :: JSON
		  ELSE doc_key_and_value_recursive.value
		END) AS t
	)SELECT data_id, key, value FROM doc_key_and_value_recursive WHERE json_typeof(value) <> ''object'' OR key = ANY(string_to_array('''||geojson_columns||''','','')) ORDER BY 2,1;'
);
				
EXECUTE format('SELECT dynamic_pivot(''SELECT data_id, key, value FROM data_table ORDER BY 1,2'',''SELECT DISTINCT key FROM data_table ORDER BY 1'',''curseur_central'');
			   		SELECT create_table_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
			   		MOVE BACKWARD FROM "curseur_central";
			   		SELECT insert_into_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
				   	CLOSE "curseur_central"'
			  );	
RAISE INFO 'exiting from feed_data_tables_from_central for table %', table_name; 

END;
$BODY$;

COMMENT ON FUNCTION feed_data_tables_from_central(text,text,text)
IS 'description : 
		Feed the tables from key/pair tables. 
	parameters :
		schema_name text	 -- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text		 -- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void
		
	comment :
		Should accept a "keys_to_ignore" parameter (as for geojson fields we want to keep as geojson).
		For the moment the function is specific to our naming convention (point, ligne, polygone)';