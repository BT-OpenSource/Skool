set mapred.child.java.opts -Xmx4096m; 
 set output.compression.enabled true; 
 set output.compression.codec org.apache.hadoop.io.compress.SnappyCodec; 
 raw_equipment = load '${nameNode}/user/cloudera/HAASDEMO_01/EMPLOYEES/landing/umcompressed_data/' USING AvroStorage();
Store raw_equipment into '${nameNode}/user/cloudera/HAASDEMO_01/EMPLOYEES/landing/DELTA_DATA/${targetDirYear}/${targetDirMonth}/${targetDirDate}/${targetDirHour}/${targetDirMinute}' USING AvroStorage(); 

