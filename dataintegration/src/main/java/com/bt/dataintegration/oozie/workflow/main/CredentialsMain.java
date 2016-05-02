package com.bt.dataintegration.oozie.workflow.main;

import java.util.LinkedList;
import java.util.List;

import com.bt.dataintegration.oozie.workflow.tags.Credential;
import com.bt.dataintegration.oozie.workflow.tags.CredentialProperty;
import com.bt.dataintegration.oozie.workflow.tags.Credentials;
import com.bt.dataintegration.property.config.HadoopConfig;

import static com.bt.dataintegration.constants.Constants.*;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class CredentialsMain {

	private CredentialProperty cpropUri = new CredentialProperty();
	private CredentialProperty cpropPrincipal = new CredentialProperty();
	private Credential cred = new Credential();
	private Credentials creds = new Credentials();
	List<CredentialProperty> propList = new LinkedList<CredentialProperty>();
	
	public Credentials setCredentialsMain(HadoopConfig hconf) {
		
		cpropUri.setName("hive2.jdbc.url");
		cpropUri.setValue("${hive2_jdbc_url}");	
		propList.add(cpropUri);
		cpropPrincipal.setName("hive2.server.principal");
		cpropPrincipal.setValue("${hive2_server_principal}");
		propList.add(cpropPrincipal);
		cred.setPropList(propList);
		cred.setName(HIVE_CREDENTIALS);
		cred.setType(HIVE_CREDENTIALS_TYPE);
		creds.setHiveCredential(cred);
		/*creds.setName("hive_credentials");
		creds.setType("hive2");*/
		
		return creds;
	}
}
