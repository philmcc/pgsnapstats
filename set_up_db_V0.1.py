#!/usr/local/bin/python2.7
#
# PGSnapStats
# Script to create the tables for PGSnapStats
#

import psycopg2




if __name__ == "__main__":

#UM_HOST='ubermon-master.live.iparadigms.com'
	UM_DBNAME='pgsnapstats'
	UM_HOST='localhost'

# Attempt to set up a connection to the database

	print "Installing pgsnapstats database tables"

	try:
		conn = psycopg2.connect("dbname= " + UM_DBNAME + " user='postgres'")
		print "Success! - connected to " + str(UM_DBNAME)
	except psycopg2.DatabaseError, e:
		print "Unable to connect to the database - " + str(UM_DBNAME)
		print 'Error %s' % e
		exit()


	try:
		print "TABLE pgsnapstats_snap"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_snap CASCADE") 
		cur.execute("CREATE TABLE pgsnapstats_snap(snapid bigint, ts timestamp, description varchar(255), end_time timestamp, hostname varchar(255), dbname varchar(255), dbid int);") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e


	try:
		print "TABLE pgsnapstats_database;"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_database CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_database (snapid bigint, dbid oid NOT NULL,  dbname varchar(255), hostname varchar(255), numbackends integer, \
			xact_commit bigint, xact_rollback bigint, blks_read bigint, blks_hit bigint, datname_id integer, CONSTRAINT pgstatspack_database_pk PRIMARY KEY (snapid, dbid));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_databases;"
		cur = conn.cursor()
		#cur.execute("DROP TABLE if exists pgsnapstats_databases CASCADE ;") 
		cur.execute("CREATE TABLE pgsnapstats_databases (dbid SERIAL, dbname varchar(255) not null, hostname varchar(255), current char, CONSTRAINT pgstatspack_databases_pk PRIMARY KEY (dbid, hostname));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e



	try:
		print "TABLE pgsnapstats_tables"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_tables CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_tables(table_snap_id SERIAL, snapid bigint NOT NULL, schema_name varchar(255), table_name varchar(255), seq_scan bigint, seq_tup_read bigint, idx_scan bigint, \
		idx_tup_fetch bigint, n_tup_ins bigint, n_tup_upd bigint, n_tup_del bigint, heap_blks_read bigint, heap_blks_hit bigint, idx_blks_read bigint, idx_blks_hit bigint, \
		toast_blks_read bigint, toast_blks_hit bigint, tidx_blks_read bigint, tidx_blks_hit bigint, n_tup_hot_upd bigint, n_live_tup bigint, n_dead_tup bigint, \
		last_vacuum timestamp with time zone, last_autovacuum timestamp with time zone, last_analyze timestamp with time zone, last_autoanalyze timestamp with time zone, \
		analyze_count bigint,autovacuum_count bigint, vacuum_count bigint, autoanalyze_count bigint, tbl_size bigint, CONSTRAINT pgsnapstats_tables_pk PRIMARY KEY (table_snap_id));") 
		
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e


	try:
		print "TABLE pgsnapstats_indexes"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_indexes CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_indexes(snapid bigint NOT NULL, schemaname varchar(255), relname varchar(255), indexrelname varchar(255), idx_scan bigint, idx_tup_read bigint, idx_tup_fetch bigint,idx_blks_read bigint, idx_blks_hit bigint,\
		CONSTRAINT pgsnapstats_indexes_pk PRIMARY KEY (snapid, schemaname, indexrelname));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_sequences"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_sequences CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_sequences(snapid bigint NOT NULL, schemaname varchar(255), relname varchar(255), seq_blks_read bigint, seq_blks_hit bigint, CONSTRAINT pgsnapstats_sequences_pk PRIMARY KEY (snapid, schemaname, relname ));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_settings"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_settings CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_settings(snapid bigint, setting_name varchar(255), setting varchar(255), source varchar(255), CONSTRAINT pgsnapstats_settings_pk PRIMARY KEY (snapid, setting_name));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_statements"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_statements CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_statements(snapid bigint NOT NULL, user_name integer, query text, calls bigint, total_time double precision, \"rows\" bigint, shared_blks_hit bigint, shared_blks_read bigint, \
			 		shared_blks_dirtied bigint, shared_blks_written bigint, local_blks_hit bigint, local_blks_read bigint, local_blks_dirtied bigint, local_blks_written bigint, temp_blks_read bigint, temp_blks_written bigint, \
					blk_read_time double precision, blk_write_time double precision, CONSTRAINT pgsnapstats_statements_pk PRIMARY KEY (snapid, user_name, query));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_functions"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_functions CASCADE;") 
		cur.execute("CREATE TABLE pgsnapstats_functions (snapid bigint NOT NULL, schemaname varchar(255), function_name varchar(255), calls bigint, total_time bigint, self_time bigint, CONSTRAINT pgsnapstats_functions_pk PRIMARY KEY (snapid, function_name));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "TABLE pgsnapstats_bgwriter"
		cur = conn.cursor()
		cur.execute("DROP TABLE if exists pgsnapstats_bgwriter CASCADE;") 
		cur.execute("create table pgsnapstats_bgwriter (snapid bigint not null, checkpoints_timed bigint, checkpoints_req bigint,checkpoint_write_time double precision, checkpoint_sync_time double precision, buffers_checkpoint bigint, buffers_clean bigint, maxwritten_clean bigint, buffers_backend bigint, \
		buffers_backend_fsync bigint, buffers_alloc bigint, stats_reset timestamp with time zone, CONSTRAINT pgsnapstats_bgwriter_pk PRIMARY KEY (snapid));") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	#try:
	#	print "SEQUENCE pgsnapstatsnameid"
	#	cur = conn.cursor()
	#	cur.execute("CREATE SEQUENCE pgsnapstatsnameid INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "TABLE pgsnapstats_names"
	#	cur = conn.cursor()
	#	cur.execute("CREATE TABLE pgsnapstats_names(dbid integer,  nameid integer not null default nextval('pgsnapstatsnameid'::text), name text, CONSTRAINT pgsnapstats_names_pkey PRIMARY KEY (nameid));") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "INDEX idx_pgsnapstats_names_name"
	#	cur = conn.cursor()
	#	cur.execute("create unique index idx_pgsnapstats_names_name on pgsnapstats_names(name);") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "TABLE pgsnapstats_version"
	#	cur = conn.cursor()
	#	cur.execute("CREATE TABLE pgsnapstats_version(version varchar(10));") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e


	try:
		print "DROP SEQUENCE if exists pgsnapstatsid;"
		cur = conn.cursor()
		cur.execute("DROP SEQUENCE if exists pgsnapstatsid;") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	try:
		print "SEQUENCE pgsnapstatsid"
		cur = conn.cursor()
		cur.execute("CREATE SEQUENCE pgsnapstatsid;") 
		conn.commit()
	except psycopg2.DatabaseError, e:
		if conn:
			conn.rollback()
		print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_database_v"
	#	cur = conn.cursor()
	#	cur.execute("CREATE OR REPLACE VIEW pgsnapstats_database_v AS SELECT snapid, dbid, dbname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit FROM pgsnapstats_database;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e
		
	#try:
	#	print "VIEW pgsnapstats_tables_v "
	#	cur = conn.cursor()
	#	cur.execute("create or replace view pgsnapstats_tables_v as SELECT snapid, name as table_name, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, heap_blks_read, heap_blks_hit, idx_blks_read, \
	#	idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit, tbl_size, idx_size FROM pgsnapstats_tables JOIN pgsnapstats_names on nameid=table_name_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_indexes_v"
	#	cur = conn.cursor()
	#	cur.execute("create view pgsnapstats_indexes_v as SELECT snapid, n1.name as index_name, n2.name as table_name, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit FROM pgsnapstats_indexes \
	#	JOIN pgsnapstats_names n1 on n1.nameid=index_name_id JOIN pgsnapstats_names n2 on n2.nameid=table_name_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_sequences_v"
	#	cur = conn.cursor()
	#	cur.execute("create view pgsnapstats_sequences_v as SELECT snapid, name as sequence_name, seq_blks_read, seq_blks_hit FROM pgsnapstats_sequences JOIN pgsnapstats_names on nameid=sequence_name_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_settings_v"
	#	cur = conn.cursor()
	#	cur.execute("create view pgsnapstats_settings_v as SELECT snapid, n1.name as name, n2.name as setting, n3.name as source FROM pgsnapstats_settings JOIN pgsnapstats_names n1 on n1.nameid=name_id \
	#	JOIN pgsnapstats_names n2 on n2.nameid=setting_id JOIN pgsnapstats_names n3 on n3.nameid=source_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_statements_v"
	#	cur = conn.cursor()
	#	cur.execute("create view pgsnapstats_statements_v as SELECT snapid, n1.name as user_name, n2.name as query, calls, total_time, \"rows\" FROM pgsnapstats_statements JOIN pgsnapstats_names n1 on \
	#	n1.nameid=user_name_id JOIN pgsnapstats_names n2 on n2.nameid=query_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "VIEW pgsnapstats_functions_v"
	#	cur = conn.cursor()
	#	cur.execute("create view pgsnapstats_functions_v as SELECT snapid, funcid, n1.name as function_name, calls, total_time, self_time FROM pgsnapstats_functions JOIN pgsnapstats_names n1 on n1.nameid=function_name_id;") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e

	#try:
	#	print "INSERT pgsnapstats_version (version number 0.1)"
	#	cur = conn.cursor()
	#	cur.execute("insert into pgsnapstats_version values('0.1');") 
	#	conn.commit()
	#except psycopg2.DatabaseError, e:
	#	if conn:
	#		conn.rollback()
	#	print 'Error %s' % e


	print "Finished insrtalling pgsnapstat tables."




	



