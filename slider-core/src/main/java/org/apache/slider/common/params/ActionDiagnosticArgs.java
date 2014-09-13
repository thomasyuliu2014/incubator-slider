package org.apache.slider.common.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = {SliderActions.ACTION_THAW},
commandDescription = SliderActions.DESCRIBE_ACTION_THAW)
public class ActionDiagnosticArgs extends AbstractActionArgs
{
	
	@Override
	public String getActionName() {
		return SliderActions.ACTION_DIAGNOSTIC;
	}
	
	  @Parameter(names = {ARG_CLIENT}, 
	      description = "print configuration of the slider client")
	  public boolean client;
	
	  @Parameter(names = {ARG_SLIDER}, 
	      description = "print configuration of the running slider app master")
	  public boolean slider;
	
	  @Parameter(names = {ARG_APPLICATION}, 
	      description = "print configuration of the running application")
	  public boolean application;
	
	  @Parameter(names = {ARG_YARN}, 
	      description = "print configuration of the YARN cluster")
	  public boolean yarn;
	
	  @Parameter(names = {ARG_CREDENTIALS}, 
	      description = "print credentials of the current user")
	  public boolean credentials;
	
	  @Parameter(names = {ARG_ALL}, 
	      description = "print all of the information above")
	  public boolean all;
	
	  @Parameter(names = {ARG_INTELLIGENT}, 
	      description = "diagnoze the application intelligently")
	  public boolean intelligent;

}
