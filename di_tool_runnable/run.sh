
file="configuration.properties"
p_dir="HDI_Password_Repository"
jceks_id="jceks://hdfs/user"

if [ -f $file ] 
then
	echo "$file found !"
		
	source=`cat $file | grep source_name | cut -d"=" -f 2`
	db_schema=`cat $file | grep database_schemaname | cut -d"=" -f 2`	
	instance=`cat $file | grep cluster_haas_instance_name | cut -d"=" -f 2`		
	
	pwd_alias="hdi.$source.$db_schema.pswd"
	pwd_dir="$jceks_id/$instance/$p_dir/$pwd_alias"
	
	pwd_date=`hadoop fs -ls /user/$instance/$p_dir/$pwd_alias | awk '{print $6}'`
	
	read -s -p "Enter DB password for user $db_schema :" pwd

echo $pwd
	
	if [  -z "$pwd_date" ]
	then
		echo "Enter your source database password - (please wait for the prompt...)"
		hadoop credential create $pwd_alias -provider $pwd_dir
	else
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
				java -cp dataintegration-0.0.1-SNAPSHOT.jar com.bt.dataintegration.property.config.DIConfigService $pwd
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
				java -cp dataintegration-0.0.1-SNAPSHOT.jar com.bt.dataintegration.property.config.DIConfigService $pwd
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
fi
