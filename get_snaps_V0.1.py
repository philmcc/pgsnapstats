#!/usr/local/bin/python2.7
#
# PGSnapStats
# Script to take a snapshot for PGSnapStats
#

import psycopg2


def pull_snapshot(v_dbname, v_hostname, connHUB, v_dbid, v_snapid):

	print "pull_snapshot"
	try:
		connTARGET = psycopg2.connect("dbname= " + v_dbname + " host=" + v_hostname + " user='postgres'")
		print "Success! - connected to " + v_dbname
	except psycopg2.DatabaseError, e:
		print "Unable to connect to the database - " + v_dbname
		print 'Error %s' % e

	curTARGET = connTARGET.cursor()
	curHUB = connHUB.cursor()

	############################
	#Build SQL Statements
	############################
	
	#pgsnapstats_database
	v_database_SQL = "SELECT d.datname, d.numbackends, d.xact_commit, d.xact_rollback, d.blks_read, d.blks_hit FROM pg_stat_database d WHERE datname = \'" + v_dbname + "\';"
	print v_database_SQL

	#pgsnapstats_tables
	v_tables_SQL = "SELECT t.schemaname, t.relname, t.seq_scan,t.seq_tup_read,t.idx_scan,t.idx_tup_fetch,t.n_tup_ins,\
		t.n_tup_upd,t.n_tup_del,it.heap_blks_read,it.heap_blks_hit, it.idx_blks_read,it.idx_blks_hit,\
		it.toast_blks_read,it.toast_blks_hit,it.tidx_blks_read,it.tidx_blks_hit,t.n_tup_hot_upd, t.n_live_tup, t.n_dead_tup, t.last_vacuum, t.last_autovacuum, t.last_analyze, t.last_autoanalyze, \
		t.analyze_count,t.autovacuum_count,t.vacuum_count, t.autoanalyze_count,pg_relation_size(t.relid) as tbl_size\
		FROM pg_statio_user_tables it, pg_stat_user_tables t JOIN pg_index i on i.indrelid=t.relid  WHERE (t.relid = it.relid) \
		GROUP BY t.schemaname,t.relname,t.seq_scan,t.seq_tup_read,t.idx_scan,t.idx_tup_fetch,t.n_tup_ins,t.n_tup_upd,t.n_tup_del, \
		it.heap_blks_read,it.heap_blks_hit,it.idx_blks_read,it.idx_blks_hit,it.toast_blks_read,it.toast_blks_hit,it.tidx_blks_read, \
		it.tidx_blks_hit,t.relid,t.n_tup_hot_upd, t.n_live_tup,t.n_dead_tup,t.last_vacuum,t.last_autovacuum, t.last_analyze, \
		t.last_autoanalyze, t.analyze_count,t.autovacuum_count, t.vacuum_count, t.autoanalyze_count;"

	#pgsnapstats_indexes
	v_indexes_SQL = "SELECT i.schemaname, i.relname, i.indexrelname, i.idx_scan, i.idx_tup_read, i.idx_tup_fetch, ii.idx_blks_read, ii.idx_blks_hit, pg_relation_size(i.relid)\
		FROM pg_stat_user_indexes i join pg_statio_user_indexes ii on i.indexrelid = ii.indexrelid;"

	#pgsnapstats_sequences
	v_sequences_SQL = "SELECT schemaname, relname, blks_read, blks_hit FROM pg_statio_user_sequences;"

	#pgsnapstats_settings
	v_settings_SQL = "SELECT name, setting, source FROM pg_settings WHERE (source != 'default');"

	#pgsnapstats_functions
	v_functions_SQL = "select schemaname, funcname, calls, total_time, self_time from pg_stat_user_functions order by total_time limit 100;"

	#pgsnapstats_bgwriter
	v_bgwriter_SQL = "select checkpoints_timed, checkpoints_req, buffers_checkpoint,checkpoint_write_time, checkpoint_sync_time, buffers_clean, \
		maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, stats_reset from pg_stat_bgwriter;"

	#pgsnapstats_statements
	v_statements_SQL = ""

	############################
	############################

	#pgsnapstats_database
	try:
		curTARGET.execute(v_database_SQL)
		v_database_result = curTARGET.fetchall()
		print v_database_result
		for row in v_database_result:
			#print row
			curHUB.execute("INSERT INTO pgsnapstats_database(snapid, dbid, dbname, hostname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)" , (str(v_snapid), str(v_dbid), str(row[1]), str(v_hostname), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5])))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e
	
	
	#pgsnapstats_tables
	try:	    
	    curTARGET.execute(v_tables_SQL)
	    v_tables_result = curTARGET.fetchall()
	    print v_tables_result
	    for row in v_tables_result:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_tables(snapid, schema_name, table_name, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del, heap_blks_read,heap_blks_hit, idx_blks_read, idx_blks_hit, toast_blks_read, toast_blks_hit, tidx_blks_read, tidx_blks_hit, n_tup_hot_upd, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, analyze_count,autovacuum_count, vacuum_count, autoanalyze_count,tbl_size) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e

	#pgsnapstats_indexes
	try:	    
	    curTARGET.execute(v_indexes_SQL)
	    v_indexes_result = curTARGET.fetchall()
	    print v_indexes_result
	    for row in v_indexes_result:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_indexes(snapid, schemaname, relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch, idx_blks_read, idx_blks_hit) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e

	#pgsnapstats_sequences
	try:	    
	    curTARGET.execute(v_sequences_SQL)
	    v_sequences_result = curTARGET.fetchall()
	    print v_sequences_result
	    for row in v_sequences_result:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_sequences(snapid, schemaname, relname, seq_blks_read, seq_blks_hit) values (%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e

	#pgsnapstats_functions
	try:	    
	    curTARGET.execute(v_functions_SQL)
	    v_functions_result = curTARGET.fetchall()
	    print v_functions_result
	    for row in v_functions_result:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_functions(snapid, schemaname, function_name, calls, total_time, self_time ) values (%s,%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3], row[4],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e

	#pgsnapstats_bgwriter
	try:	    
	    curTARGET.execute(v_bgwriter_SQL)
	    bgwriter = curTARGET.fetchall()
	    print bgwriter
	    for row in bgwriter:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_bgwriter(snapid, checkpoints_timed, checkpoints_req, buffers_checkpoint, checkpoint_write_time, checkpoint_sync_time, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, stats_reset ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e	

	#pgsnapstats_settings
	try:	    
	    curTARGET.execute(v_settings_SQL)
	    v_settings_result = curTARGET.fetchall()
	    print v_settings_result
	    for row in v_settings_result:
	    	print row
	    	curHUB.execute("INSERT INTO pgsnapstats_settings(snapid, setting_name, setting, source) values (%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2],))
	except psycopg2.DatabaseError, e:
		print 'Error %s' % e

	#pgsnapstats_statements
	#try:	    
	#    curTARGET.execute(v_settings_SQL)
	#    v_settings_result = curTARGET.fetchall()
	#    print v_settings_result
	#    for row in v_settings_result:
	#    	print row
	#    	curHUB.execute("INSERT INTO pgsnapstats_settings(snapid, ) values (%s,%s,%s,%s,%s)" ,(v_snapid, row[0], row[1], row[2], row[3],))
	#except psycopg2.DatabaseError, e:
#		print 'Error %s' % e	


	#curHUB.execute("SELECT current_timestamp;") 
	#v_MY_time_now = curHUB.fetchone()[0]
	#print v_MY_time_now
	connHUB.commit()
	connTARGET.close()
	return 1

#################################
######## END Of pull_snapshot
#################################


def snapshot_all_dbs():
	print "snapshot_all_dbs"
	# Get a list of all dbs
	# loop through connecting to each and taking a snapshot of each one
	# Write data back to pgsnapstats


	DBNAME='pgsnapstats'

	# Attempt to set up a connection to the database
	try:
		connHUB = psycopg2.connect("dbname= " + DBNAME + " user='postgres'")
		print "Success! - connected to " + str(DBNAME)
	except psycopg2.DatabaseError, e:
		print "Unable to connect to the database - " + str(DBNAME)
		print 'Error %s' % e
		exit()

	curHUB = connHUB.cursor()
	try:
		v_sql="select distinct dbname, hostname, dbid from pgsnapstats_databases where current ='Y';"
		curHUB.execute(v_sql)
	except:
		print "Couldn't get a list of databases."
	v_databases = curHUB.fetchall()
	print v_databases

	for db in v_databases:
		#print db
		v_dbname = str(db[0])
		#print "dbname - " + str(db[0])
		v_host = str(db[1])
		#print "host   - " + str(db[1])
		v_dbid = str(db[2])
		#print "dbid   - " + str(db[2])
		curHUB.execute("SELECT current_timestamp;") 
		v_time_now = curHUB.fetchone()[0]
		print "Time Now = " + str(v_time_now)
  		curHUB.execute("SELECT nextval('pgsnapstatsid');")
  		v_snap_id = curHUB.fetchone()[0]
  		print "Snap_ID = " + str(v_snap_id)
  		#CREATE TABLE pgsnapstats_snap(snapid bigint, ts timestamp, description varchar(255), end_time timestamp, hostname varchar(255), dbname varchar(255), dbid int)
  		v_snap_sql= "INSERT INTO pgsnapstats_snap(snapid, ts, dbname, dbid) values(" + str(v_snap_id) + ", \'" + str(v_time_now) + "', '" + str(db[0]) +"', " +  str(db[2]) + ");"
  		print v_snap_sql
  		curHUB.execute(v_snap_sql)
		
		
		# Create and generate the snapshot for this db
		v_result = pull_snapshot(v_dbname, v_host, connHUB, v_dbid, v_snap_id)
		
		connHUB.commit()

	return 1

#################################
######## END Of snapshot_all_dbs
#################################

if __name__ == "__main__":


	v_snaps = snapshot_all_dbs()
