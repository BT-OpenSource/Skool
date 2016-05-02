/*
 * Author: 609298143 (Manish Bajaj)
 */

package com.bt.dataintegration.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.bt.dataintegration.property.config.*;

public interface IDBConnect {

	//Connect to Oracle Database reading properties file
	public Connection connect(DIConfig diConfig);
	
	//Check if table exists, if not intimate user
	public boolean validateTable(Connection con,DIConfig conf) throws Exception;
	
	//If table exists, get column_name and datatype for all the columns
	public Map<String, List<String>> getColumnDetails(Connection con, DIConfig conf) throws SQLException;		
	
	//Fetch table size
	public double getTableSize(Connection con,DIConfig conf) throws SQLException;
	
	//De-scoped for now (FTE)
	public Map<String, List<String>> getTablePartition(Connection con, DIConfig conf) throws SQLException;
	
	//Fetch Table constraints
	public Map<String, String> getTableConstraints(Connection con,DIConfig conf) throws SQLException;	
}
