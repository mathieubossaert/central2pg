rm -f ./central2pg.sql
cat documentation.txt schema_creation.sql table_central_authentication_tokens.sql get_fresh_token_from_central.sql get_token_from_central.sql dynamic_pivot.sql does_index_exists.sql \
create_table_from_refcursor.sql insert_into_from_refcursor.sql get_form_tables_list_from_central.sql get_submission_from_central.sql feed_data_tables_from_central.sql \
get_file_from_central.sql odk_central_to_pg.sql get_form_version.sql create_draft.sql push_media_to_central.sql publish_form_version.sql > central2pg.sql
                                                                                                                                                                   