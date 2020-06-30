#!/usr/bin/env python
#
# config_mgmt.py
# Provides a class for configuration validation and for Dynamic Port Breakout.

try:

    # import from sonic-cfggen, re use this
    from imp import load_source
    load_source('sonic_cfggen', '/usr/local/bin/sonic-cfggen')
    from sonic_cfggen import deep_update, FormatConverter, sort_data
    from swsssdk import ConfigDBConnector, SonicV2Connector, port_util
    from pprint import PrettyPrinter, pprint
    from json import dump, load, dumps, loads
    from sys import path as sysPath
    from os import path as osPath
    from os import system
    from datetime import datetime
    from time import sleep as tsleep

    import sonic_yang
    import syslog
    import re

except ImportError as e:
    raise ImportError("%s - required module not found" % str(e))

# Globals
# This class may not need to know about YANG_DIR ?, sonic_yang shd use
# default dir.
YANG_DIR = "/usr/local/yang-models"
CONFIG_DB_JSON_FILE = '/etc/sonic/confib_db.json'
# TODO: Find a place for it on sonic switch.
DEFAULT_CONFIG_DB_JSON_FILE = '/etc/sonic/port_breakout_config_db.json'

# Class to handle config managment for SONIC, this class will use PLY to verify
# config for the commands which are capable of change in config DB.

class ConfigMgmt():

    def __init__(self, source="configDB", debug=False, allowTablesWithOutYang=True):

        try:
            self.configdbJsonIn = None
            self.configdbJsonOut = None
            self.allowTablesWithOutYang = allowTablesWithOutYang

            # logging vars
            self.SYSLOG_IDENTIFIER = "ConfigMgmt"
            self.DEBUG = debug

            self.sy = sonic_yang.sonic_yang(YANG_DIR, debug=debug)
            # load yang models
            self.sy.loadYangModel()
            # load jIn from config DB or from config DB json file.
            if source.lower() == 'configdb':
                self.readConfigDB()
            # treat any other source as file input
            else:
                self.readConfigDBJson(source)
            # this will crop config, xlate and load.
            self.loadData(self.configdbJsonIn)

        except Exception as e:
            print(e)
            raise(Exception('ConfigMgmt Class creation failed'))

        return

    def __del__(self):
        pass

    """
    Return tables loaded in config for which YANG model does not exist.
    """
    def tablesWithOutYang(self):

        return self.sy.tablesWithOutYang

    """
    Explicit function to load config data in Yang Data Tree
    """
    def loadData(self, configdbJson):
        self.sy.load_data(configdbJson)
        # Raise if tables without YANG models are not allowed but exist.
        if not self.allowTablesWithOutYang and len(self.sy.tablesWithOutYang):
            raise Exception('Config has tables without YANG models')

        return

    """
    Validate current Data Tree
    """
    def validateConfigData(self):

        try:
            self.sy.validate_data_tree()
        except Exception as e:
            self.sysLog(msg='Data Validation Failed')
            return False

        print('Data Validation successful')
        self.sysLog(msg='Data Validation successful')
        return True

    """
    syslog Support
    """
    def sysLog(self, debug=syslog.LOG_INFO, msg=None):

        # log debug only if enabled
        if self.DEBUG == False and debug == syslog.LOG_DEBUG:
            return
        syslog.openlog(self.SYSLOG_IDENTIFIER)
        syslog.syslog(debug, msg)
        syslog.closelog()

        return

    def readConfigDBJson(self, source=CONFIG_DB_JSON_FILE):

        print('Reading data from {}'.format(source))
        self.configdbJsonIn = readJsonFile(source)
        #print(type(self.configdbJsonIn))
        if not self.configdbJsonIn:
            raise(Exception("Can not load config from config DB json file"))
        self.sysLog(msg='Reading Input {}'.format(self.configdbJsonIn))

        return

    """
        Get config from redis config DB
    """
    def readConfigDB(self):

        print('Reading data from Redis configDb')

        # Read from config DB on sonic switch
        db_kwargs = dict(); data = dict()
        configdb = ConfigDBConnector(**db_kwargs)
        configdb.connect()
        deep_update(data, FormatConverter.db_to_output(configdb.get_config()))
        self.configdbJsonIn =  FormatConverter.to_serialized(data)
        self.sysLog(syslog.LOG_DEBUG, 'Reading Input from ConfigDB {}'.\
            format(self.configdbJsonIn))

        return

    def writeConfigDB(self, jDiff):
        print('Writing in Config DB')
        """
        On Sonic Switch
        """
        db_kwargs = dict(); data = dict()
        configdb = ConfigDBConnector(**db_kwargs)
        configdb.connect(False)
        deep_update(data, FormatConverter.to_deserialized(jDiff))
        data = sort_data(data)
        self.sysLog(msg="Write in DB: {}".format(data))
        configdb.mod_config(FormatConverter.output_to_db(data))

        return

# End of Class ConfigMgmt

"""
    Config MGMT class for Dynamic Port Breakout(DPB).
    This is derived from ConfigMgmt.
"""
class ConfigMgmtDPB(ConfigMgmt):

    def __init__(self, source="configDB", debug=False, allowTablesWithOutYang=True):

        try:
            ConfigMgmt.__init__(self, source=source, debug=debug, \
                allowTablesWithOutYang=allowTablesWithOutYang)
            self.oidKey = 'ASIC_STATE:SAI_OBJECT_TYPE_PORT:oid:0x'

        except Exception as e:
            print(e)
            raise(Exception('ConfigMgmtDPB Class creation failed'))

        return

    def __del__(self):
        pass

    """
      Check if a key exists in ASIC DB or not.
    """
    def checkKeyinAsicDB(self, key, db):

        self.sysLog(msg='Check Key in Asic DB: {}'.format(key))
        try:
            # chk key in ASIC DB
            if db.exists('ASIC_DB', key):
                return True
        except Exception as e:
            print(e)
            raise(e)

        return False

    def testRedisCli(self, key):
        # To Debug
        if self.DEBUG:
            cmd = 'sudo redis-cli -n 1 hgetall "{}"'.format(key)
            self.sysLog(syslog.LOG_DEBUG, "Running {}".format(cmd))
            print(cmd)
            system(cmd)
        return

    """
     Check ASIC DB for PORTs in port List
     ports: List of ports
     portMap: port to OID map.
     Return: True, if all ports are not present.
    """
    def checkNoPortsInAsicDb(self, db, ports, portMap):
        try:
            # connect to ASIC DB,
            db.connect(db.ASIC_DB)
            for port in ports:
                key = self.oidKey + portMap[port]
                if self.checkKeyinAsicDB(key, db) == False:
                    # Test again via redis-cli
                    self.testRedisCli(key)
                else:
                    return False

        except Exception as e:
            print(e)
            return False

        return True

    """
    Verify in the Asic DB that port are deleted,
    Keep on trying till timeout period.
    db = database, ports, portMap, timeout
    """
    def verifyAsicDB(self, db, ports, portMap, timeout):

        print("Verify Port Deletion from Asic DB, Wait...")
        self.sysLog(msg="Verify Port Deletion from Asic DB, Wait...")

        try:
            for waitTime in range(timeout):
                self.sysLog(msg='Check Asic DB: {} try'.format(waitTime+1))
                # checkNoPortsInAsicDb will return True if all ports are not
                # present in ASIC DB
                if self.checkNoPortsInAsicDb(db, ports, portMap):
                    break
                tsleep(1)

            # raise if timer expired
            if waitTime + 1 == timeout:
                print("!!!  Critical Failure, Ports are not Deleted from \
                    ASIC DB, Bail Out  !!!")
                self.sysLog(syslog.LOG_CRIT, "!!!  Critical Failure, Ports \
                    are not Deleted from ASIC DB, Bail Out  !!!")
                raise(Exception("Ports are present in ASIC DB after timeout"))

        except Exception as e:
            print(e)
            raise e

        return True

    def breakOutPort(self, delPorts=list(), addPorts= list(), portJson=dict(), \
        force=False, loadDefConfig=True):

        MAX_WAIT = 60
        try:
            # delete Port and get the Config diff, deps and True/False
            delConfigToLoad, deps, ret = self.deletePorts(ports=delPorts, \
                force=force)
            # return dependencies if delete port fails
            if ret == False:
                return deps, ret

            # add Ports and get the config diff and True/False
            addConfigtoLoad, ret = self.addPorts(ports=addPorts, \
                portJson=portJson, loadDefConfig=loadDefConfig)
            # return if ret is False, Great thing, no change is done in Config
            if ret == False:
                return None, ret

            # Save Port OIDs Mapping Before Deleting Port
            dataBase = SonicV2Connector(host="127.0.0.1")
            if_name_map, if_oid_map = port_util.get_interface_oid_map(dataBase)
            self.sysLog(syslog.LOG_DEBUG, 'if_name_map {}'.format(if_name_map))

            # If we are here, then get ready to update the Config DB as below:
            # -- shutdown the ports,
            # -- Update deletion of ports in Config DB,
            # -- verify Asic DB for port deletion,
            # -- then update addition of ports in config DB.
            self._shutdownIntf(delPorts)
            self.writeConfigDB(delConfigToLoad)
            # Verify in Asic DB,
            self.verifyAsicDB(db=dataBase, ports=delPorts, portMap=if_name_map, \
                timeout=MAX_WAIT)
            self.writeConfigDB(addConfigtoLoad)

        except Exception as e:
            print(e)
            return None, False

        return None, True

    """
    Delete all ports.
    delPorts: list of port names.
    force: if false return dependecies, else delete dependencies.

    Return:
    WithOut Force: (xpath of dependecies, False) or (None, True)
    With Force: (xpath of dependecies, False) or (None, True)
    """
    def deletePorts(self, ports=list(), force=False):

        configToLoad = None; deps = None
        try:
            self.sysLog(msg="delPorts ports:{} force:{}".format(ports, force))

            print('\nStart Port Deletion')
            deps = list()

            # Get all dependecies for ports
            for port in ports:
                xPathPort = self.sy.findXpathPortLeaf(port)
                print('Find dependecies for port {}'.format(port))
                self.sysLog(msg='Find dependecies for port {}'.format(port))
                # print("Generated Xpath:" + xPathPort)
                dep = self.sy.find_data_dependencies(str(xPathPort))
                if dep:
                    deps.extend(dep)

            # No further action with no force and deps exist
            if force == False and deps:
                return configToLoad, deps, False;

            # delets all deps, No topological sort is needed as of now, if deletion
            # of deps fails, return immediately
            elif deps and force:
                for dep in deps:
                    self.sysLog(msg='Deleting {}'.format(dep))
                    self.sy.delete_node(str(dep))
            # mark deps as None now,
            deps = None

            # all deps are deleted now, delete all ports now
            for port in ports:
                xPathPort = self.sy.findXpathPort(port)
                print("Deleting Port: " + port)
                self.sysLog(msg='Deleting Port:{}'.format(port))
                self.sy.delete_node(str(xPathPort))

            # Let`s Validate the tree now
            if self.validateConfigData()==False:
                return configToLoad, deps, False;

            # All great if we are here, Lets get the diff
            self.configdbJsonOut = self.sy.get_data()
            # Update configToLoad
            configToLoad = self.updateDiffConfigDB()

        except Exception as e:
            print(e)
            print("Port Deletion Failed")
            return configToLoad, deps, False

        return configToLoad, deps, True

    """
    Add Ports and default config for ports to config DB, after validation of
    data tree

    PortJson: Config DB Json Part of all Ports same as PORT Table of Config DB.
    ports = list of ports
    loadDefConfig: If loadDefConfig add default config as well.

    return: Sucess: True or Failure: False
    """
    def addPorts(self, ports=list(), portJson=dict(), loadDefConfig=True):

        configToLoad = None
        try:
            self.sysLog(msg='Start Port Addition')
            self.sysLog(msg="addPorts ports:{} loadDefConfig:{}".\
                format(ports, loadDefConfig))
            self.sysLog(msg="addPorts Args portjson {}".format(portJson))

            print('\nStart Port Addition')
            # get default config if forced
            defConfig = dict()
            if loadDefConfig:
                defConfig = self.getDefaultConfig(ports)
                self.sysLog(msg='Default Config: {}'.format(defConfig))

            # get the latest Data Tree, save this in input config, since this
            # is our starting point now
            self.configdbJsonIn = self.sy.get_data()

            # Get the out dict as well, if not done already
            if self.configdbJsonOut is None:
                self.configdbJsonOut = self.sy.get_data()

            # update portJson in configdbJsonOut PORT part
            self.configdbJsonOut['PORT'].update(portJson['PORT'])
            # merge new config with data tree, this is json level merge.
            # We do not allow new table merge while adding default config.
            if loadDefConfig:
                print("Merge Default Config for {}".format(ports))
                self.sysLog(msg="Merge Default Config for {}".format(ports))
                self.mergeConfigs(self.configdbJsonOut, defConfig, True)

            # create a tree with merged config and validate, if validation is
            # sucessful, then configdbJsonOut contains final and valid config.
            self.loadData(self.configdbJsonOut)
            if self.validateConfigData()==False:
                return configToLoad, False

            # All great if we are here, Let`s get the diff and update COnfig
            configToLoad = self.updateDiffConfigDB()

        except Exception as e:
            print(e)
            print("Port Addition Failed")
            return configToLoad, False

        return configToLoad, True

    """
    Based on the list of Ports, create a dict to shutdown port, update Config DB.
    Input: [] of ports.
    """
    def _shutdownIntf(self, ports):
        """ shut down all the interfaces before deletion """
        shutDownConf = dict(); shutDownConf["PORT"] = dict()
        for intf in ports:
            shutDownConf["PORT"][intf] = {"admin_status": "down"}
        self.sysLog(msg='shutdown Interfaces: {}'.format(shutDownConf))

        if len(shutDownConf["PORT"]):
            self.writeConfigDB(shutDownConf)

        return

    """
    Merge second dict in first, Note both first and second dict will be changed
    First Dict will have merged part D1 + D2
    Second dict will have D2 - D1 [unique keys in D2]
    Unique keys in D2 will be merged in D1 only if uniqueKeys=True
    """
    def mergeConfigs(self, D1, D2, uniqueKeys=True):

        try:
            def mergeItems(it1, it2):
                if isinstance(it1, list) and isinstance(it2, list):
                    it1.extend(it2)
                elif isinstance(it1, dict) and isinstance(it2, dict):
                    self.mergeConfigs(it1, it2)
                elif isinstance(it1, list) or isinstance(it2, list):
                    raise ("Can not merge Configs, List problem")
                elif isinstance(it1, dict) or isinstance(it2, dict):
                    raise ("Can not merge Configs, Dict problem")
                else:
                    #print("Do nothing")
                    # First Dict takes priority
                    pass
                return

            for it in D1.keys():
                #print(it)
                # D2 has the key
                if D2.get(it):
                    mergeItems(D1[it], D2[it])
                    del D2[it]
                # D2  does not have the keys
                else:
                    pass

            # if uniqueKeys are needed, merge rest of the keys of D2 in D1
            if uniqueKeys:
                D1.update(D2)
        except Exce as e:
            print("Merge Config failed")
            print(e)
            raise e

        return D1

    """
    search Relevant Keys in Config using DFS, This function is mainly
    used to search port related config in Default ConfigDbJson file.
    In: Config to be searched
    skeys: Keys to be searched in In Config i.e. search Keys.
    Out: Contains the search result
    """
    def searchKeysInConfig(self, In, Out, skeys):

        found = False
        if isinstance(In, dict):
            for key in In.keys():
                #print("key:" + key)
                for skey in skeys:
                    # pattern is very specific to current primary keys in
                    # config DB, may need to be updated later.
                    pattern = '^' + skey + '\|' + '|' + skey + '$' + \
                        '|' + '^' + skey + '$'
                    #print(pattern)
                    reg = re.compile(pattern)
                    #print(reg)
                    if reg.search(key):
                        # In primary key, only 1 match can be found, so return
                        # print("Added key:" + key)
                        Out[key] = In[key]
                        found = True
                        break
                # Put the key in Out by default, if not added already.
                # Remove later, if subelements does not contain any port.
                if Out.get(key) is None:
                    Out[key] = type(In[key])()
                    if self.searchKeysInConfig(In[key], Out[key], skeys) == False:
                        del Out[key]
                    else:
                        found = True

        elif isinstance(In, list):
            for skey in skeys:
                if skey in In:
                    found = True
                    Out.append(skey)
                    #print("Added in list:" + port)

        else:
            # nothing for other keys
            pass

        return found

    """
    This function returns the relavant keys in Input Config.
    For Example: All Ports related Config in Config DB.
    """
    def configWithKeys(self, configIn=dict(), keys=list()):

        configOut = dict()
        try:
            if len(configIn) and len(keys):
                self.searchKeysInConfig(configIn, configOut, skeys=keys)
        except Exception as e:
            print("configWithKeys Failed, Error: {}".format(str(e)))
            raise e

        return configOut

    """
    Create a defConfig for given Ports from Default Config File.
    """
    def getDefaultConfig(self, ports=list()):

        # function code
        try:
            print("Generating default config for {}".format(ports))
            defConfigIn = readJsonFile(DEFAULT_CONFIG_DB_JSON_FILE)
            #print(defConfigIn)
            defConfigOut = dict()
            self.searchKeysInConfig(defConfigIn, defConfigOut, skeys=ports)
        except Exception as e:
            print("getDefaultConfig Failed, Error: {}".format(str(e)))
            raise e

        return defConfigOut

    def updateDiffConfigDB(self):

        # main code starts here
        configToLoad = dict()
        try:
            # Get the Diff
            print('Generate Final Config to write in DB')
            configDBdiff = self.diffJson()
            # Process diff and create Config which can be updated in Config DB
            configToLoad = self.createConfigToLoad(configDBdiff, \
                self.configdbJsonIn, self.configdbJsonOut)

        except Exception as e:
            print("Config Diff Generation failed")
            print(e)
            raise e

        return configToLoad

    """
    Create the config to write in Config DB from json diff
    diff: diff in input config and output config.
    inp: input config before delete/add ports.
    outp: output config after delete/add ports.
    """
    def createConfigToLoad(self, diff, inp, outp):

        ### Internal Functions ###
        """
        Handle deletes in diff dict
        """
        def deleteHandler(diff, inp, outp, config):

            # if output is dict, delete keys from config
            if isinstance(inp, dict):
                for key in diff:
                    # make sure keys from diff are present in inp but not in outp
                    # then delete it.
                    if key in inp and key not in outp:
                        if type(inp[key]) == list:
                            # assign current lists as empty.
                            config[key] = []
                        else:
                            # assign key to None(null), redis will delete entire key
                            config[key] = None
                    else:
                        # log such keys
                        print("Diff: Probably wrong key: {}".format(key))

            elif isinstance(inp, list):
                # just take list from output
                # print("Delete from List: {} {} {}".format(inp, outp, list))
                #print(type(config))
                config.extend(outp)

            return

        """
        Handle inserts in diff dict
        """
        def insertHandler(diff, inp, outp, config):

            # if outp is a dict
            if isinstance(outp, dict):
                for key in diff:
                    #print(key)
                    # make sure keys are only in outp
                    if key not in inp and key in outp:
                        # assign key in config same as outp
                        config[key] = outp[key]
                    else:
                        # log such keys
                        print("Diff: Probably wrong key: {}".format(key))

            elif isinstance(outp, list):
                # just take list from output
                # print("Delete from List: {} {} {}".format(inp, outp, list))
                config.extend(outp)

            return

        """
        Recursively iterate diff to generate config to write in configDB
        """
        def recurCreateConfig(diff, inp, outp, config):

            changed = False
            # updates are represented by list in diff and as dict in outp\inp
            # we do not allow updates right now
            if isinstance(diff, list) and isinstance(outp, dict):
                return changed

            idx = -1
            for key in diff:
                #print(key)
                idx = idx + 1
                if str(key) == '$delete':
                    deleteHandler(diff[key], inp, outp, config)
                    changed = True
                elif str(key) == '$insert':
                    insertHandler(diff[key], inp, outp, config)
                    changed = True
                else:
                    # insert in config by default, remove later if not needed
                    if isinstance(diff, dict):
                        # config should match with outp
                        config[key] = type(outp[key])()
                        if recurCreateConfig(diff[key], inp[key], outp[key], \
                            config[key]) == False:
                            del config[key]
                        else:
                            changed = True
                    elif isinstance(diff, list):
                        config.append(key)
                        if recurCreateConfig(diff[idx], inp[idx], outp[idx], \
                            config[-1]) == False:
                            del config[-1]
                        else:
                            changed = True

            return changed

        ### Function Code ###
        try:
            configToLoad = dict()
            #import pdb; pdb.set_trace()
            recurCreateConfig(diff, inp, outp, configToLoad)

        except Exception as e:
            print("Create Config to load in DB, Failed")
            print(e)
            raise e

        # if debug
        #with open('configToLoad.json', 'w') as f:
        #    dump(configToLoad, f, indent=4)

        return configToLoad

    def diffJson(self):

        from jsondiff import diff
        return diff(self.configdbJsonIn, self.configdbJsonOut, syntax='symmetric')

# end of class ConfigMgmtDPB

"""
    Test Functions:
    Below Test provides unit test funtionalities to test config_mgmt without
    intergration with Python click.
    These tests can be executed on Sonic Switch or in VS environment. User needs
    to run these test by running this test script directly. i.e.
    python config_mgmt.py

    Prerequisite: To have a config loaded with Ethernet0 in 4X25G/1x100G/2x50G
    mode. Also default config file must be placed at /etc/sonic.
"""

# Read given JSON file
def readJsonFile(fileName):
    #print(fileName)
    try:
        with open(fileName) as f:
            result = load(f)
    except Exception as e:
        raise Exception(e)

    return result

# print pretty
prt = PrettyPrinter(indent=4)
def prtprint(obj):
    prt.pprint(obj)
    return

def getPortConfigDB():
    # Read from config DB on sonic switch
    db_kwargs = dict(); data = dict()
    configdb = ConfigDBConnector(**db_kwargs)
    configdb.connect()
    deep_update(data, FormatConverter.db_to_output(configdb.get_table('PORT')))

    return data


def testRun_Delete_Add_Port(cmode, nmode, loadDef):

    ## Params for Ethernet 0, Note Test is only for one PORT
    delPortDict = {
        '4x25G[10G]' : ['Ethernet0', 'Ethernet1', 'Ethernet2', 'Ethernet3'],
        '1x100G[40G]': ['Ethernet0'],
        '2x50G': ['Ethernet0', 'Ethernet2']
    }

    portJsonDict = {
        '4x25G[10G]': {
            "PORT": {
                "Ethernet0": {
                    "alias": "Eth1/1",
                    "description": "",
                    "index": "0",
                    "lanes": "65",
                    "speed": "25000"
                },
                "Ethernet1": {
                    "alias": "Eth1/2",
                    "description": "",
                    "index": "0",
                    "lanes": "66",
                    "speed": "25000"
                },
                "Ethernet2": {
                    "alias": "Eth1/3",
                    "description": "",
                    "index": "0",
                    "lanes": "67",
                    "speed": "25000"
                },
                "Ethernet3": {
                    "alias": "Eth1/4",
                    "description": "",
                    "index": "0",
                    "lanes": "68",
                    "speed": "25000"
                }
            }
        },
        '1x100G[40G]': {
            "PORT": {
                "Ethernet0": {
                        "alias": "Eth1/1",
                        "admin_status": "up",
                        "lanes": "65,66,67,68",
                        "description": "",
                        "speed": "100000"
                }
            }
        },
        '2x50G': {
            "PORT": {
                "Ethernet0": {
                        "alias": "Eth1/1",
                        "admin_status": "up",
                        "lanes": "65,66",
                        "description": "",
                        "speed": "50000"
                },
                "Ethernet2": {
                        "alias": "Eth1/3",
                        "admin_status": "up",
                        "lanes": "67,68",
                        "description": "",
                        "speed": "50000"
                }
            }
        }
    }

    # TODO: Verify config in Config DB  after writing to config DB.
    print('Test Run Break Out Ports')
    try:
        cm = ConfigMgmtDPB(source='configDB', debug=True)

        delPorts = delPortDict[cmode]
        addPorts = delPortDict[nmode]
        portJson = portJsonDict[nmode].copy()

        cm.breakOutPort(delPorts=delPorts, addPorts= addPorts, portJson=portJson,\
            force=True, loadDefConfig=loadDef)

        print("\n***Port Break Out Test Passed***\n")

    except Exception as e:
        print(e)
        print("\n***Port Break Out Test Failed***\n")
        return

    return

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Test Dy Port BreakOut Without \
                Python click Integration',
                formatter_class=argparse.RawTextHelpFormatter, epilog="""
Usage:
python config_mgmt.py --currect-mode(-c) $CUR_MODE --new-mode(-n) $NEW_MODE
""")

    parser.add_argument('-c', '--currect-mode', type=str, \
        help='Current Mode of PORT', required=True, \
        choices=['4x25G[10G]', '1x100G[40G]', '2x50G'])
    parser.add_argument('-n', '--new-mode', type=str, \
        help='New Mode of PORT', required=True, \
        choices=['4x25G[10G]', '1x100G[40G]', '2x50G'])
    parser.add_argument('-l', '--load-default', type=bool, \
        help='Load Default Config if True', required=True, \
        choices=[True, False])

    args = parser.parse_args()

    #fill the args
    cmode = args.currect_mode
    nmode = args.new_mode
    loadDef = args.load_default
    if cmode == nmode:
        print("Current Mode of PORT is same as ew Mode")
        return

    testRun_Delete_Add_Port(cmode, nmode, loadDef)

    return

if __name__ == '__main__':
    main()
