


/*
FUNCTION: dynamic_pivot(text, text, refcursor)
	description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		Creates a pivot table dynamically, without specifying mannually the row structure.
		Returns a cursor use by both following function to create a table and fill it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor
*/

CREATE OR REPLACE FUNCTION dynamic_pivot(
	central_query text,
	headers_query text,
	INOUT cname refcursor DEFAULT NULL::refcursor)
    RETURNS refcursor
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  left_column text;
  header_column text;
  value_column text;
  h_value text;
  headers_clause text;
  query text;
  j json;
  r record;
  i int:=1;
BEGIN
  -- find the column names of the source query
  EXECUTE 'select row_to_json(_r.*) from (' ||  central_query || ') AS _r' into j;
  FOR r in SELECT * FROM json_each_text(j)
  LOOP
    IF (i=1) THEN left_column := r.key;
      ELSEIF (i=2) THEN header_column := r.key;
      ELSEIF (i=3) THEN value_column := r.key;
    END IF;
    i := i+1;
  END LOOP;

  -- build the dynamic transposition quer, based on the canonical model
  -- (CASE WHEN...)
  FOR h_value in EXECUTE headers_query
  LOOP
    headers_clause := concat(headers_clause,
     format(chr(10)||',min(case when %I=%L then %I::text end) as %I',
           header_column,
	   h_value,
	   value_column,
	   h_value ));
  END LOOP;

  query := format('SELECT %I %s FROM (select *,row_number() over() as rn from (%s) AS _c) as _d GROUP BY %I order by min(rn)',
           left_column,
	   headers_clause,
	   central_query,
	   left_column);

  -- open the cursor so the caller can FETCH right away.
  -- if cname is not null it will be used as the name of the cursor,
  -- otherwise a name "<unnamed portal unique-number>" will be generated.
  OPEN cname FOR execute query;
END
$BODY$;

COMMENT ON FUNCTION dynamic_pivot(text, text,refcursor) IS 'description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		Creates a pivot table dynamically, without specifying mannually the row structure.
		Returns a cursor use by both following function to create a table and fill it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor';