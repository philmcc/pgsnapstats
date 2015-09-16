--bgwriter
CREATE VIEW v_bgwritter as
 select b.snapid, s.dbname, 
 				b.checkpoints_timed as "Scheduled Checkpoint Performed", 
 				b.checkpoints_req as "Requested Checkpoints Performed", 
 				b.checkpoint_write_time as "Time Spend WritingCheckpoints", 
 				b.checkpoint_sync_time as "Time Spend Syncronizing Checkpoints",
 				b.buffers_checkpoint as "Number of Buffers Written During Checkpoints",
 				b.buffers_clean as "Number of Buffers Written by BG Writter",
 				b.maxwritten_clean as "No Times Scan stopped due to too many Buffers Written",
 				b.buffers_backend as "No Buffers Written by Backend",
 				b.buffers_backend_fsync as "No Times Backend Called fsync",
 				b.buffers_alloc as "No Buffers Allocated",
 				b.stats_reset as "Last Stats Reste Time"
 from pgsnapstats_bgwriter b, pgsnapstats_snap s
 where s.snapid = b.snapid
 GROUP BY b.snapid, s.dbname;




 --snap
 snap(snapid bigint, ts timestamp with time zone, description varchar(255), end_time timestamp with time zone, hostname varchar(255), dbname varchar(255), dbid int);"

 select dbname, hostname, snapid, to_char(ts, 'YYYY-MM-DD HH:MI'), end_time, description from pgsnapstats_snap order by dbname, ts desc;