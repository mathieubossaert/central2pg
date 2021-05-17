-- FUNCTION: odk_central.feed_data_tables_from_central(text, text)

-- DROP FUNCTION odk_central.feed_data_tables_from_central(text, text);

/* 
	Should accept a "keys_to_ignore" parameter (as for geojson fields we want to keep as geojson).
	For the moment the function is specific to our naming convention (point, ligne, polygone)
*/

CREATE OR REPLACE FUNCTION odk_central.feed_data_tables_from_central(
	schema_name text,	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
	table_name text	-- the table containing plain json submission from the get_submission_from_central() function call
    )
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare requete_a text;
declare requete_b text;
declare requete_c text;
declare columns_list text;
BEGIN
EXECUTE format('
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
		  WHEN json_typeof(doc_key_and_value_recursive.value) <> ''object'' OR key IN (''point'',''ligne'',''polygone'') THEN ''{}'' :: JSON
		  ELSE doc_key_and_value_recursive.value
		END) AS t
	)SELECT data_id, key, value FROM doc_key_and_value_recursive WHERE json_typeof(value) <> ''object'' OR key IN (''point'',''ligne'',''polygone'') ORDER BY 2,1;'
);

requete_a = 'SELECT data_id, key, value FROM data_table ORDER BY 1,2';
requete_b = 'SELECT DISTINCT key FROM data_table ORDER BY 1';
requete_c = concat('SELECT odk_central.dynamic_pivot('''||requete_a||''',''', requete_b||''',''curseur_central'');
			   		SELECT odk_central.create_table_from_refcursor('''||schema_name||'.'||table_name||'_data'', ''curseur_central'');
			   		MOVE BACKWARD FROM "curseur_central";
			   		SELECT odk_central.insert_into_from_refcursor('''||schema_name||'.'||table_name||'_data'', ''curseur_central'');
				   	CLOSE "curseur_central"');
EXECUTE (requete_c);
END;
$BODY$;
