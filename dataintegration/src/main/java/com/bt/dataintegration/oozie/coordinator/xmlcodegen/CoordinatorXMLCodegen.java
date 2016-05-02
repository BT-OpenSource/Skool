package com.bt.dataintegration.oozie.coordinator.xmlcodegen;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;

import com.bt.dataintegration.oozie.coordinator.main.ActionMain;
import com.bt.dataintegration.oozie.coordinator.main.ControlsMain;
import com.bt.dataintegration.oozie.coordinator.main.CoordinatorMain;
import com.bt.dataintegration.oozie.coordinator.main.WorkflowMain;
import com.bt.dataintegration.oozie.coordinator.tags.Action;
import com.bt.dataintegration.oozie.coordinator.tags.Controls;
import com.bt.dataintegration.oozie.coordinator.tags.Coordinator;
import com.bt.dataintegration.oozie.coordinator.tags.Workflow;
import com.bt.dataintegration.oozie.workflow.xmlcodegen.WorkflowXMLCodegen;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

public class CoordinatorXMLCodegen {

	final static Logger logger = Logger.getLogger(CoordinatorXMLCodegen.class);
	
	public void generateXML(HadoopConfig hconf) {
		
		//HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();
		Workflow workflow = new WorkflowMain().setWorkflowMain();
		Controls ctl = new ControlsMain().setControlsMain();
		Action act = new ActionMain().setActionMain(workflow);
		Coordinator coord = new CoordinatorMain().setCoordinatorMain(ctl, act, hconf);
		
		try {

			logger.info("Generating Oozie coordinator.xml...");
			String tableName = null;
			if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
				tableName = hconf.getTableName();
			} else if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
				tableName = hconf.getHiveTableName();
			}
			File file = new File(tableName +"/coordinator.xml");
			JAXBContext jaxbContext = JAXBContext
					.newInstance(Coordinator.class);
			javax.xml.bind.Marshaller jaxbMarshaller = jaxbContext
					.createMarshaller();

			// output pretty printed
			jaxbMarshaller.setProperty(
					javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, true);

			jaxbMarshaller.marshal(coord, file);
			//jaxbMarshaller.marshal(workflow, System.out);			
			logger.info("coordinator.xml generated !");
			
			DirectoryHandler.sendFileToHDFS(hconf, "coordinator.xml");
			DirectoryHandler.givePermissionToHDFSFile(hconf, "coordinator.xml");
			
		} catch (JAXBException e) {
			logger.error("Error generating XML for oozie Workflow", e);
		}
	}
}
