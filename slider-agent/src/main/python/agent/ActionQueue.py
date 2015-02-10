#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import Queue

import logging
import traceback
import threading
import pprint
import os
import time
import subprocess
import getpass

from AgentConfig import AgentConfig
from AgentToggleLogger import AgentToggleLogger
from CommandStatusDict import CommandStatusDict
from CustomServiceOrchestrator import CustomServiceOrchestrator
import Constants


logger = logging.getLogger()
installScriptHash = -1

class ActionQueue(threading.Thread):
  """ Action Queue for the agent. We pick one command at a time from the queue
  and execute it
  """

  STATUS_COMMAND = 'STATUS_COMMAND'
  EXECUTION_COMMAND = 'EXECUTION_COMMAND'

  IN_PROGRESS_STATUS = 'IN_PROGRESS'
  COMPLETED_STATUS = 'COMPLETED'
  FAILED_STATUS = 'FAILED'

  STORE_APPLIED_CONFIG = 'record_config'
  AUTO_RESTART = 'auto_restart'
  
  command_path = ''
  imageName = ''
  options = ''
  hostPort = ''
  containerPort = ''
  mounting_directory = ''
  start_command = ''
  additional_param = ''

  def __init__(self, config, controller, agentToggleLogger):
    super(ActionQueue, self).__init__()
    self.queueOutAgentToggleLogger = agentToggleLogger
    self.queueInAgentToggleLogger = AgentToggleLogger("info")
    self.commandQueue = Queue.Queue()
    self.commandStatuses = CommandStatusDict(callback_action=
    self.status_update_callback)
    self.config = config
    self.controller = controller
    self._stop = threading.Event()
    self.tmpdir = config.getResolvedPath(AgentConfig.APP_TASK_DIR)
    self.customServiceOrchestrator = CustomServiceOrchestrator(config,
                                                               controller,
                                                               self.queueOutAgentToggleLogger)


  def stop(self):
    self._stop.set()

  def stopped(self):
    return self._stop.isSet()

  def put(self, commands):
    for command in commands:
      self.queueInAgentToggleLogger.adjustLogLevelAtStart(command['commandType'])
      message = "Adding " + command['commandType'] + " for service " + \
                command['serviceName'] + " of cluster " + \
                command['clusterName'] + " to the queue."
      self.queueInAgentToggleLogger.log(message)
      self.queueInAgentToggleLogger.adjustLogLevelAtEnd(command['commandType'])
      logger.debug(pprint.pformat(command))
      self.commandQueue.put(command)

  def empty(self):
    return self.commandQueue.empty()


  def run(self):
    while not self.stopped():
      time.sleep(2)
      command = self.commandQueue.get() # Will block if queue is empty
      self.queueOutAgentToggleLogger.adjustLogLevelAtStart(command['commandType'])
      self.process_command(command)
      self.queueOutAgentToggleLogger.adjustLogLevelAtEnd(command['commandType'])
    logger.info("ActionQueue stopped.")

  def get_tmpdir(self):
      return self.tmpdir[-30:-2]

  def process_command(self, command):
    logger.debug("Took an element of Queue: " + pprint.pformat(command))
    # make sure we log failures
    try:
      if command['commandType'] == self.EXECUTION_COMMAND:
        self.execute_command(command)
      elif command['commandType'] == self.STATUS_COMMAND:
        self.execute_status_command(command)
      else:
        logger.error("Unrecognized command " + pprint.pformat(command))
    except Exception, err:
      # Should not happen
      traceback.print_exc()
      logger.warn(err)


  def execute_command(self, command):
      
    logger.info("aaa execution command " + str(command))
      
    '''
    Executes commands of type  EXECUTION_COMMAND
    '''
    clusterName = command['clusterName']
    commandId = command['commandId']

    message = "Executing command with id = {commandId} for role = {role} of " \
              "cluster {cluster}".format(
      commandId=str(commandId), role=command['role'],
      cluster=clusterName)
    taskId = command['taskId']

    # if auto generated then do not report result
    reportResult = CommandStatusDict.shouldReportResult(command)

    # Preparing 'IN_PROGRESS' report
    in_progress_status = self.commandStatuses.generate_report_template(command)
    in_progress_status.update({
      'tmpout': self.tmpdir + os.sep + 'output-' + str(taskId) + '.txt',
      'tmperr': self.tmpdir + os.sep + 'errors-' + str(taskId) + '.txt',
      'structuredOut': self.tmpdir + os.sep + 'structured-out-' + str(
        taskId) + '.json',
      'status': self.IN_PROGRESS_STATUS,
      'reportResult': reportResult
    })
    self.commandStatuses.put_command_status(command, in_progress_status, reportResult)

    store_config = False
    if ActionQueue.STORE_APPLIED_CONFIG in command['commandParams']:
      store_config = 'true' == command['commandParams'][ActionQueue.STORE_APPLIED_CONFIG]
    store_command = False
    if 'roleParams' in command and ActionQueue.AUTO_RESTART in command['roleParams']:
      store_command = 'true' == command['roleParams'][ActionQueue.AUTO_RESTART]
    logger.info("command fromhost: " + str(command))
    
    if store_command:
      logger.info("Component has indicated auto-restart. Saving details from START command.")

    if 'configurations' in command:
        logger.info(str( command['configurations']))
        if 'docker' in command['configurations']:
            logger.info(str( command['configurations']['docker']))
            if 'docker.command_path' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.command_path'])
                self.command_path = command['configurations']['docker']['docker.command_path']
            if 'docker.image_name' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.image_name'])
                self.imageName = command['configurations']['docker']['docker.image_name']
            if 'docker.options' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.options'])
                self.options = command['configurations']['docker']['docker.options']
            if 'docker.container_port' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.container_port'])
                self.containerPort = command['configurations']['docker']['docker.container_port']
            if 'docker.mounting_directory' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.mounting_directory'])
                self.mounting_directory = command['configurations']['docker']['docker.mounting_directory']
            if 'docker.start_command' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.start_command'])
                self.start_command = command['configurations']['docker']['docker.start_command']
            if 'docker.additional_param' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.additional_param'])
                self.additional_param = command['configurations']['docker']['docker.additional_param']
        
    if command['roleCommand'] == 'INSTALL':
        docker_command = ["/usr/bin/docker", "pull", self.imageName]
        logger.info("docker install: " + str(docker_command))
        proc = subprocess.Popen(docker_command, stdout = subprocess.PIPE)
        out = proc.communicate()
        logger.info(str(out))
        
    if command['roleCommand'] == 'START':
        docker_command = [self.command_path, "run"]
        if self.options:
            docker_command = docker_command + self.options.split(" ")
        if self.containerPort:
            docker_command.append("-p")
            self.hostPort = '11911'
            docker_command.append(self.hostPort+":"+self.containerPort)
        if self.mounting_directory:
            docker_command.append("-v")
            docker_command.append(self.tmpdir+":"+self.mounting_directory)
        docker_command.append("-name")
        docker_command.append(self.get_tmpdir())
        docker_command.append(self.imageName)
        if self.start_command:
            docker_command.append(self.start_command)
        if self.additional_param:
            docker_command = docker_command + self.additional_param.split(" ")
        logger.info("docker run" + str(docker_command))
        
        proc = subprocess.Popen(docker_command, stdout = subprocess.PIPE)
        out = proc.communicate()
        logger.info(str(out))
        
    """
    commandresult = self.customServiceOrchestrator.runCommand(command,
                                                              
                                                              
                                                              in_progress_status[
                                                                'tmpout'],
                                                              in_progress_status[
                                                                'tmperr'],
                                                              True,
                                                              store_config or store_command)
    """
    # If command is STOP then set flag to indicate stop has been triggered.
    # In future we might check status of STOP command and take other measures
    # if graceful STOP fails (like force kill the processes)
    
    commandresult = {Constants.EXIT_CODE:0, 'stdout':'', 'stderr':''}
    
    # dumping results
    status = self.COMPLETED_STATUS
    if commandresult[Constants.EXIT_CODE] != 0:
      status = self.FAILED_STATUS
    roleResult = self.commandStatuses.generate_report_template(command)
    roleResult.update({
      'stdout': commandresult['stdout'],
      'stderr': commandresult['stderr'],
      Constants.EXIT_CODE: commandresult[Constants.EXIT_CODE],
      'status': status,
      'reportResult': reportResult
    })
    if roleResult['stdout'] == '':
      roleResult['stdout'] = 'None'
    if roleResult['stderr'] == '':
      roleResult['stderr'] = 'None'

    if 'structuredOut' in commandresult:
      roleResult['structuredOut'] = str(commandresult['structuredOut'])
    else:
      roleResult['structuredOut'] = ''
      # let server know that configuration tags were applied
    if status == self.COMPLETED_STATUS:
      if 'configurationTags' in command:
        roleResult['configurationTags'] = command['configurationTags']
      if Constants.ALLOCATED_PORTS in commandresult:
        roleResult['allocatedPorts'] = commandresult[Constants.ALLOCATED_PORTS]
      if Constants.FOLDERS in commandresult:
        roleResult['folders'] = commandresult[Constants.FOLDERS]
    self.commandStatuses.put_command_status(command, roleResult, reportResult)

  # Store action result to agent response queue
  def result(self):
    return self.commandStatuses.generate_report()

  def execute_status_command(self, command):
    status_command = ''
    if 'configurations' in command:
        logger.info(str( command['configurations']))
        if 'docker' in command['configurations']:
            logger.info(str( command['configurations']['docker']))
            if 'docker.status_command' in command['configurations']['docker']:
                logger.info( command['configurations']['docker']['docker.status_command'])
                status_command = command['configurations']['docker']['docker.status_command']
                
    logger.info("aaa status command" + str(command))
    docker_command = ["/usr/bin/docker", "exec"]
    docker_command.append(self.get_tmpdir())
    docker_command.append(status_command)
    
    '''
    proc = subprocess.Popen(docker_command, stdout = subprocess.PIPE)
    out = proc.communicate()
    logger.info("docker exec" + str(docker_command))
    '''
    
    '''
    Executes commands of type STATUS_COMMAND
    '''
    try:
      cluster = command['clusterName']
      service = command['serviceName']
      component = command['componentName']
      reportResult = CommandStatusDict.shouldReportResult(command)
      component_status = self.customServiceOrchestrator.requestComponentStatus(command)

      result = {"componentName": component,
                "msg": "",
                "clusterName": cluster,
                "serviceName": service,
                "reportResult": reportResult,
                "roleCommand": command['roleCommand']
      }

      if 'configurations' in component_status:
        result['configurations'] = component_status['configurations']
      if Constants.EXIT_CODE in component_status:
        result['status'] = component_status[Constants.EXIT_CODE]
        logger.debug("Got live status for component " + component + \
                     " of service " + str(service) + \
                     " of cluster " + str(cluster))
        logger.debug(pprint.pformat(result))

      if result is not None:
        self.commandStatuses.put_command_status(command, result, reportResult)
    except Exception, err:
      traceback.print_exc()
      logger.warn(err)
    pass


  def status_update_callback(self):
    """
    Actions that are executed every time when command status changes
    """
    self.controller.heartbeat_wait_event.set()
