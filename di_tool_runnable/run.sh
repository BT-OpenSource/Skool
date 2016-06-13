
file="configuration/configuration.properties"
p_dir="HDI_Password_Repository"
jceks_id="jceks://hdfs/user"

if [ -f $file ] 
then
	echo "Fetching details from the $file"
	
	import_export_file_flag=`cat $file | grep import_export_file_flag | cut -d"=" -f 2`
	if [ $import_export_file_flag == 1 ]
	then	
		source=`cat $file | grep source_name | cut -d"=" -f 2`
		db_schema=`cat $file | grep database_schemaname | cut -d"=" -f 2`
		instance=`cat $file | grep  hdfs_instance_name | cut -d"=" -f 2`	
		hostname=`cat $file | grep database_host | cut -d"=" -f 2`		
		db_schema=`echo $db_schema | tr 'a-z' 'A-Z'`
		
		pwd_alias="hdi_$hostname"_"$source""_$db_schema.pswd"
		pwd_dir="$jceks_id/$instance/$p_dir/$pwd_alias"
		
		if hadoop fs -ls /user/$instance/$p_dir
		then
		echo "HDI_PASSWORD_REPOSITORY exists."
		else
		hadoop fs -mkdir /user/$instance/$p_dir
		hadoop fs -chmod 660 /user/$instance/$p_dir
		fi
		
		pwd_date=`hadoop fs -ls /user/$instance/$p_dir/$pwd_alias | awk '{print $6}'`
		read -s -p "Enter DB password for user $db_schema :" pwd
		
		if [  -z "$pwd_date" ]
		then
			echo $'\n'
			echo "Enter your source database password - (please wait for the prompt...)"
			hadoop credential create $pwd_alias -provider $pwd_dir
			java -cp dataintegration-0.0.1-SNAPSHOT.jar:configuration/ojdbc6-11.2.0.3.jar:/opt/cloudera/parcels/CDH/jars/* com.bt.dataintegration.property.config.DIConfigService $pwd
		else
			echo $'\n'
			echo "This password was last updated on - $pwd_date"
			echo "Do you wish to change it ? (y/n)"
			
			read opt
			if [[ $opt == "y" || $opt == "Y" ]]
			then
				echo "Deleting old password file..."
				hadoop credential delete $pwd_alias -provider $pwd_dir
				hadoop fs -rm -R $pwd_dir
				echo "Creating new encrypted password..."
				hadoop credential create $pwd_alias -provider $pwd_dir
				if [ ! -z "$pwd" ]
				then
					java -cp dataintegration-0.0.1-SNAPSHOT.jar:configuration/ojdbc6-11.2.0.3.jar:/opt/cloudera/parcels/CDH/jars/* com.bt.dataintegration.property.config.DIConfigService $pwd
				else
					echo "Database password cannot be null."
					echo "Please provide password as argument in this script."
					exit
				fi
			elif [[ $opt == "n" || $opt == "N" ]]
			then
				echo "Password alias for sqoop import: $pwd_alias"
				echo "Your encrypted database password will be stored with above alias in the directory: $pwd_dir"
				
				if [ ! -z "$pwd" ]
				then
					java -cp dataintegration-0.0.1-SNAPSHOT.jar:configuration/ojdbc6-11.2.0.3.jar:/opt/cloudera/parcels/CDH/jars/* com.bt.dataintegration.property.config.DIConfigService $pwd
				else
					echo "Database password cannot be null."
					echo "Please provide password as argument in this script."
					exit
				fi	
			else
				echo "Exiting code..."
				exit
			fi
		fi
	elif [ $import_export_file_flag == "3" ]
	then
		java -cp dataintegration-0.0.1-SNAPSHOT.jar:/opt/cloudera/parcels/CDH/jars/* com.bt.dataintegration.property.config.DIFileSystemService
	fi	
fi
