from typing import List

import subprocess
import time
import _thread
import yaml
import argparse
import os
import json
import copy

# #TODO parse node config file name https://stackoverflow.com/questions/8384737/extract-file-name-from-path-no-matter-what-the-os-path-format
def executePowershellCommand(command: str) -> str:
    """
    Executes a PowerShell command and returns the output.

    Parameters:
    command (str): The PowerShell command to execute.

    Returns:
    str: The output from the PowerShell command.
    """
    try:
        # Use subprocess to run the PowerShell command
        result = subprocess.run(["powershell",'-NoProfile', "-Command", command], 
                                capture_output=True, text=True, check=True)
        return result.stdout.strip()  # Return the standard output
    except subprocess.CalledProcessError as e:
        return f"Error: {e.stderr.strip()}"  # Return the error if the command fails

class Worker:
  internal_name:str
  vm_name:str
  max_run_time_seconds:int
  min_delay_after_run_seconds: int

  valid:bool = False
  running:bool = False
  ip:str
  mac_address:str

  def __init__(self,internal_name:str, name:str, max_run_time_seconds:int, min_delay_after_run_seconds:int):
    self.internal_name = internal_name
    self.vm_name = name
    self.max_run_time_seconds = max_run_time_seconds
    self.min_delay_after_run_seconds = min_delay_after_run_seconds
  @classmethod
  def fromConfigFilePath(cls, path:str):
    raise NotImplementedError
    return cls()
  def getInfo(self):
    raise NotImplementedError
  def start(self):
    raise NotImplementedError  
  def shutdown(self):
    raise NotImplementedError  
  def restart(self, delay_seconds = 30):
    self.getInfo()
    if self.running == True:
      self.shutdown()
      time.sleep(delay_seconds)
    else:
      print(f'[worker.restart] {self.vm_name} no need to shutdown')
    self.start()
  def __str__(self)->str:
    return str(f"{self.internal_name}: {self.vm_name}")
class HyperVWorker(Worker):
  def getInfo(self):
    output = executePowershellCommand(f'get-vm -Name {self.vm_name}')
    info = output.split('\n')[2]
    splitted_info = info.split()
    if splitted_info[0] == self.vm_name:
      self.valid = True
    else:
      return
    self.status = splitted_info[1]
    if self.status == 'Running':
      self.running = True
    elif self.status == 'Off':
      self.running = False
    output = executePowershellCommand(f"Get-VM -Name {self.vm_name} |select -ExpandProperty networkadapters | select macaddress")
    self.mac_address:str = output.split()[2]
    splitted_mac = self.mac_address.lower()
    splitted_mac = [splitted_mac[i:i+2] for i in range(0, len(splitted_mac), 2)]
    splitted_mac = '-'.join(splitted_mac)
    output = executePowershellCommand(f'arp -a | findstr {splitted_mac}')
    self.ip = output.split()[0]
  def shutdown(self, force = True):
    command = f'Stop-VM -Name "{self.vm_name}"'
    if force:
      command += ' -Force'
    executePowershellCommand(command)
  def start(self):
    command = f'Start-VM -Name "{self.vm_name}"'
    executePowershellCommand(command)


WORKER_TYPE_DICT = {
  "hyperv": HyperVWorker
}
WORKER_TYPE_DICT_KEYS = list(WORKER_TYPE_DICT.keys())
DEFAULT_CONFIG = {
  "name": "",
  "type": "hyperv",
  "max_run_time_seconds": 0,
  "min_delay_after_run_seconds": 0,
  "execute_while_running": "",
}
def getWorkerObjectFromConfig(internal_name:str ,config:dict)->Worker:
  print(f'parsing worker config {config}')
  config_params = {}
  default_config_keys = DEFAULT_CONFIG.keys()
  for key in default_config_keys:
    config_params[key] = DEFAULT_CONFIG[key]
  for key in config.keys():
    value = config[key]
    if key in default_config_keys:
      config_params[key] = value
    else:
      print(f'{key}: {config[key]}, key is not supported, ignoring it ')
  worker_name = config_params["name"]
  worker_type = config_params["type"]
  max_run_time_seconds, min_delay_after_run_seconds = config["max_run_time_seconds"], config['min_delay_after_run_seconds']
  if worker_name == "":
    raise ValueError('name cannot be empty')
  if worker_type not in WORKER_TYPE_DICT_KEYS:
    raise ValueError(f'incorrect worker type {worker_type}, supported types are: {" ".join(WORKER_TYPE_DICT_KEYS)}') 
    # raise ValueError(f'{config_params["type"]} is not supported, possible values are \\n-{"\\n-".join(WORKER_TYPE_DICT_KEYS)}') 
  return WORKER_TYPE_DICT[worker_type](internal_name, worker_name, max_run_time_seconds, min_delay_after_run_seconds)

class NodeConfig:
  def __init__(self, config:dict) -> None:
    self.checkIfConfigValid(config)
    self.raw:dict = config.get("node")
    self.worker_names = self.raw.get("workers")
  @classmethod
  def parseConfigFromPath(cls, path_to_config:str):
    with open(path_to_config, 'r') as file:
      config:dict = yaml.safe_load(file)
    return cls(config)
  @staticmethod
  def checkIfConfigValid(config:dict):
    config["node"]
    config["node"]["workers"]
    list(map(lambda worker: config[worker], config["node"]["workers"]))
DEFAULT_WORKER_CACHE = {
  "started_at": 0,
  "will_finish_at": 0,
  "possible_to_start_after": 0
}
class NodeCache:
  do_not_save_properties = [
    'unique_id',
    'folder_dir',
    'file_path',
    "config_path",
  ]
  filename = 'node.json'
  config_file_name = "config.json"
  cache_name_for_display = 'NodeCache'
  def __init__(self, unique_id: str, cache_folder_dir: str = './cache', reset: bool = False):
    self.unique_id = unique_id
    if not os.path.exists(cache_folder_dir):
      os.makedirs(cache_folder_dir)
    self.folder_dir = f'./{cache_folder_dir}/{self.unique_id}'
    self.file_path = self.folder_dir + f'/{self.filename}'
    self.config_path = self.folder_dir + f'/{self.config_file_name}'
    if reset is True:
      print(f'[cache] resetting {self.cache_name_for_display}')
      self.reset()
    else:
      try:
        self.load()
        print(f'[cache] loaded from file {self.cache_name_for_display}')
      except Exception:
        print(f'[cache] cant load {self.cache_name_for_display} from file, creating new')
        self.reset()
    self.customInit()
  def customInit(self):
    return True
  def reset(self):
    self.data = {}
    self.workers = {}
    self.save()
  def load(self):
    if not os.path.exists(self.folder_dir):
      os.makedirs(self.folder_dir)
    file = open(self.file_path, encoding='utf-8')
    config = json.load(file)
    file.close()
    # assign
    self.fromJson(config)
  def save(self):
    file = open(self.file_path, 'w', encoding='utf-8')
    json_to_save = self.toJson()
    time_now = time.time()
    json_to_save['update_time'] = time_now 
    self.update_time = time_now
    json.dump(json_to_save, file, ensure_ascii=False, indent=4)
    file.close()
  def toJson(self):
    some_dict = copy.copy(vars(self))
    for key in self.do_not_save_properties:
      del some_dict[key]
    return some_dict
  def fromJson(self, config:dict):
    for key, value in config.items():
      setattr(self, key, value )
    # self.stash_tabs_data = config['stash_tabs_data']
  def checkConfigChange(self, current_config:dict):
    try:
      file = open(self.config_path, encoding='utf-8')
      config = json.load(file)
      file.close()
    except FileNotFoundError:
      file = open(self.config_path, 'w', encoding='utf-8')
      json.dump(current_config, file, ensure_ascii=False, indent=4)
      file.close()
      print(f'[cache.checkConfigChange] config file wasnt saved')
      return
    if current_config == config:
      print(f'[cache.checkConfigChange] config wasnt changed')
      return 

    # remove missing workers
    for worker in self.workers: 
      if not worker in current_config["node"]["workers"]:
        print(f'{worker} was removed, clearing')
        del self.workers[worker]
    # add new workers
    for worker in current_config["node"]["workers"]: 
      if worker in self.workers:
        print(f'{worker} is new, adding')
        self.workers[worker] = DEFAULT_WORKER_CACHE
    
    raise NotImplementedError('if config was changed')


    file = open(self.config_path, 'w', encoding='utf-8')
    json.dump(current_config, file, ensure_ascii=False, indent=4)
    file.close()
class Node:
  workers:List[Worker] = []
  config:NodeConfig
  cache:NodeCache
  def __init__(self,path_to_config:str):
    with open(path_to_config, 'r') as file:
      config:dict = yaml.safe_load(file)
    self.name = path_to_config.split('/')[-1].split('.')[0]
    self.config = NodeConfig(config)
    # self.config = NodeConfig.parseConfigFromPath(path_to_config=path_to_config)
    self.vm_keys = self.config.worker_names
    for key in self.vm_keys:
      worker_config = config.get(key, None)
      if worker_config == None:
        raise Exception(f'No definition for worker with name  "{key}"')
      worker = getWorkerObjectFromConfig(key,config=worker_config)
      self.workers.append(worker)
    list(map(lambda worker: worker.getInfo(), self.workers))
    self.valid_workers = list(filter(lambda worker: worker.valid == True, self.workers))
    print(f'[node] valid workers are {list(map(lambda w: w.internal_name,self.valid_workers))}')
    self.cache = NodeCache(self.name)
    self.cache.checkConfigChange(current_config=config)
  @classmethod
  def fromYamlFile(cls,path_to_config:str):
    with open(path_to_config, 'r') as file:
      config:dict = yaml.safe_load(file)
    return cls(config)
  def getWorkerCanRun(self) -> Worker:
    for worker in self.workers:
      worker_cache = self.cache.workers.get(worker.internal_name)
      started_at = worker_cache['started_at']
      will_finish_at = worker_cache['will_finish_at']
      possible_to_start_after = worker_cache['possible_to_start_after']
      
      # always return worker which can run forever
      if worker.max_run_time_seconds == 0:
        return worker
      # return if it still can be run
      if time.time() < will_finish_at:
        return worker
      # return if delay passed
      if time.time() > possible_to_start_after:
        return worker
      # if didnt run at all
      if started_at == 0:
        return worker

    return None

  def run(self):
    if len(self.valid_workers) == 0:
      raise Exception('[node] Cannot run node scheduler for node with 0 valid workers')
    current_active_worker:Worker|None = None
    while True:
      next_active_worker:Worker|None = self.getWorkerCanRun()
      if next_active_worker != None and current_active_worker != next_active_worker:
        worker_cache = self.cache.workers.get(next_active_worker.internal_name)
        print(f'worker {str(next_active_worker)} is starting at {time.time()}')
        other_workers = list(filter(lambda worker: worker != next_active_worker, self.workers))
        if other_workers:
          print(f'Turning off other workers of the node {list(map(lambda w: str(w),other_workers))}')
          list(map(lambda worker: worker.shutdown(), other_workers))
        print(f'Starting worker {next_active_worker}')
        if time.time() < worker_cache['will_finish_at']:
          print(f'continuing previous session which was started at {worker_cache["started_at"]} ')
        else:
          print(f'starting new session')
          worker_cache['started_at'] = time.time()
          cycle_over_after = time.time() + next_active_worker.max_run_time_seconds
          worker_cache['will_finish_at'] = cycle_over_after
          worker_cache['possible_to_start_after'] = cycle_over_after + next_active_worker.min_delay_after_run_seconds
          self.cache.save()
        next_active_worker.start()
        current_active_worker = next_active_worker

      
      if current_active_worker:
        time.sleep(14)
        current_active_worker.getInfo()
        worker_status_text = f"worker:{current_active_worker} running:{current_active_worker.running}" 
        if current_active_worker.max_run_time_seconds != 0:
          time_to_run_left = int(worker_cache['will_finish_at'] - time.time()) 
          worker_status_text += f' going run for {time_to_run_left} seconds more'
          # print(worker_cache)
          if time_to_run_left < 0:
            print(f'worker time exceed, shutting down')
            current_active_worker.shutdown()
            current_active_worker = None
        else:
          worker_status_text += " going to run till next worker will be avaliable"
        print(worker_status_text, end="\r")
      else:
        print('no active workers')
      time.sleep(1)
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='')
  parser.add_argument('--file_path', type=str, help='Input file path', default='./default.yml', required=False)
  args = parser.parse_args()
  file_path = args.file_path

  node = Node(file_path)
  # node = Node.fromYamlFile(file_path)
  node.run()