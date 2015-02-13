import logging
import os
import subprocess
from AgentConfig import AgentConfig
import Constants

logger = logging.getLogger()

class DockerManager():
    
    def __init__(self, tmpdir, workroot):
        self.tmpdir = tmpdir
        self.workroot = workroot
    
    def execute_command(self, command):
        if command['roleCommand'] == 'INSTALL':
            self.pull_image(command)
        if command['roleCommand'] == 'START':
            self.start_container(command)    
        # fake
        return {Constants.EXIT_CODE:0, 'stdout':'', 'stderr':''}
            
    def pull_image(self, command):
        logger.info(str( command['configurations']))
        command_path = self.extract_config_from_command(command, 'docker.command_path')
        imageName = self.extract_config_from_command(command, 'docker.image_name')
        
        docker_command = [command_path, 'pull', imageName]
        logger.info("docker pull command: " + str(docker_command))
        self.execute_command_on_linux(docker_command)
        
    
    def extract_config_from_command(self, command, field):
        value = ''
        if 'configurations' in command:
            if 'docker' in command['configurations']:
                if field in command['configurations']['docker']:
                    logger.info(field + ': ' + str( command['configurations']['docker'][field]))
                    value = command['configurations']['docker'][field]
        return value
    
    
    # will evolve into a class hierarch, linux and windows
    def execute_command_on_linux(self, docker_command):
        proc = subprocess.Popen(docker_command, stdout = subprocess.PIPE)
        out = proc.communicate()
        logger.info("docker command output: " + str(out))
    
    
    def start_container(self, command):
        #extracting param needed by docker run from the command passed from AM
        command_path = self.extract_config_from_command(command, 'docker.command_path')
        imageName = self.extract_config_from_command(command, 'docker.image_name')
        options = self.extract_config_from_command(command, 'docker.options')
        containerPort = self.extract_config_from_command(command, 'docker.container_port')
        mounting_directory = self.extract_config_from_command(command, 'docker.mounting_directory')
        additional_param = self.extract_config_from_command(command, 'docker.additional_param')
        input_file_local_path = self.extract_config_from_command(command, 'docker.input_file.local_path')
        input_file_mount_path = self.extract_config_from_command(command, 'docker.input_file.mount_path')
        
        docker_command = [command_path, "run"]
        if options:
            docker_command = self.add_docker_run_options_to_command(docker_command, options)
        if containerPort:
            self.add_port_binding_to_command(docker_command, containerPort)
        if mounting_directory:
            self.add_mnted_dir_to_command(docker_command, "/docker_use", mounting_directory)
        if input_file_local_path:
            self.add_mnted_dir_to_command(docker_command, "/inputDir", input_file_mount_path)
        self.add_container_name_to_command(docker_command)
        docker_command.append(imageName)
        if additional_param:
            docker_command = self.add_additional_param_to_command(docker_command, additional_param)
        logger.info("docker run command: " + str(docker_command))
        self.execute_command_on_linux(docker_command)
    
    def add_docker_run_options_to_command(self, docker_command, options):
        return docker_command + options.split(" ")
    
    def add_port_binding_to_command(self, docker_command, containerPort):
        docker_command.append("-p")
        # fake
        hostPort = '11911'
        docker_command.append(hostPort+":"+containerPort)
        
    def add_mnted_dir_to_command(self, docker_command, host_dir, container_dir):
        docker_command.append("-v")
        tmp_mount_dir = self.workroot + host_dir
        docker_command.append(tmp_mount_dir+":"+container_dir)
    
    def add_container_name_to_command(self, docker_command):
        docker_command.append("-name")
        docker_command.append(self.get_container_id())
        
    def add_additional_param_to_command(self, docker_command, additional_param):
        return docker_command + additional_param.split(" ")
    
    def get_container_id(self):
        # will make this more resilient to changes
        return self.tmpdir[-30:-2]

    
    def query_status(self, command):
        status_command = self.extract_config_from_command(command, 'docker.status_command').split(" ")
        logger.info("status command" + str(status_command))
        self.execute_command_on_linux(status_command)
        #fake
        return {Constants.EXIT_CODE:0}
        
        