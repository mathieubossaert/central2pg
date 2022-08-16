```sql
	WITH RECURSIVE _tree (submission_id, key, value, type) AS (

		SELECT
		form_data->>'__id' as submission_id,
			null as key
			,form_data::jsonb
			,'object'
		from odk_central.sicen_2022
			UNION ALL
		(
			WITH typed_values AS (
				SELECT submission_id, key, jsonb_typeof(value) as typeof, value 
				FROM _tree
			)
			SELECT submission_id, CONCAT(tv.key, '.', v.key), v.value, jsonb_typeof(v.value)
			FROM typed_values as tv, LATERAL jsonb_each(value) v
			WHERE typeof = 'object'
				UNION ALL
			SELECT submission_id, CONCAT(tv.key, '[', n-1, ']'), element.val, jsonb_typeof(element.val)
			FROM typed_values as tv, LATERAL jsonb_array_elements(value) WITH ORDINALITY as element (val, n)
			WHERE typeof = 'array'
		)
	)
	SELECT DISTINCT submission_id, key, value #>> '{}' as value, type
	FROM _tree
	WHERE submission_id = 'uuid:89e68f78-632f-4236-aa07-267c99521632' --une soumission avec plusieurs fils
	AND key IS NOT NULL AND type = 'array'
	ORDER BY submission_id, key
```

Tester avce ceci pour récuperer les elements de type array et les traiter classiquement ensuite
Et une requete equivalent pour la table "submission" avce tous les element non array
