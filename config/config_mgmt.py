#!/usr/bin/env python
#
# config_mgmt.py
# Provides a class for configuration validation and for Dynamic Port Breakout.

try:

    # import from sonic-cfggen, re use this
    from imp import load_source
    load_source('sonic_cfggen', '/usr/local/bin/sonic-cfggen')
    from sonic_cfggen import deep_update, FormatConverter, sort_data
    from swsssdk import ConfigDBConnector
    from pprint import PrettyPrinter, pprint
    from json import dump, load, dumps, loads
    from sys import path as sysPath
    from os import path as osPath
    from os import system
    from datetime import datetime

    import sonic_yang
    import re

except ImportError as e:
    raise ImportError("%s - required module not found" % str(e))

# Globals
# This class may not need to know about YANG_DIR ?, sonic_yang shd use
# default dir.
YANG_DIR = "/usr/local/yang-models"
CONFIG_DB_JSON_FILE = '/etc/sonic/confib_db.json'
# TODO: Find a place for it on sonic switch.
DEFAULT_CONFIG_DB_JSON_FILE = '/etc/sonic/default_config_db.json'

# Class to handle config managment for SONIC, this class will use PLY to verify
# config for the commands which are capable of change in config DB.

class configMgmt():

    def __init__(self, source="configDB", debug=False):

        try:
            self.configdbJsonIn = None
            self.configdbJsonOut = None

            self.DEBUG_FILE = None
            if debug:
                self.DEBUG_FILE = '_debug_config_mgmt'
                with open(self.DEBUG_FILE, 'w') as df:
                    df.write('--- Start config_mgmt logging ---\n\n')

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
            self.sy.load_data(self.configdbJsonIn)

        except Exception as e:
            print(e)
            raise(Exception('configMgmt Class creation failed'))

        return

    def logInFile(self, header="", obj=None, json=False):

        if self.DEBUG_FILE:
            with open(self.DEBUG_FILE, 'a') as df:
                time = datetime.now()
                df.write('\n\n{}: {}\n'.format(time, header))
                if json:
                    dump(obj, df, indent=4)
                elif obj:
                    #print(obj)
                    df.write('{}: {}'.format(time, obj))
                df.write('\n----')

        return

    def readConfigDBJson(self, source=CONFIG_DB_JSON_FILE):

        print('Reading data from {}'.format(source))
        self.configdbJsonIn = readJsonFile(source)
        #print(type(self.configdbJsonIn))
        if not self.configdbJsonIn:
            raise(Exception("Can not load config from config DB json file"))
        self.logInFile('Reading Input', self.configdbJsonIn, True)

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
        self.logInFile('Reading Input', self.configdbJsonIn, True)

        return

    """
    Delete all ports.
    delPorts: list of port names.
    force: if false return dependecies, else delete dependencies.

    Return:
    WithOut Force: (xpath of dependecies, False) or (None, True)
    With Force: (xpath of dependecies, False) or (None, True)
    """
    def deletePorts(self, delPorts=list(), force=False):

        try:
            self.logInFile("delPorts ports:{} force:{}".format(delPorts, force))

            print('\nStart Port Deletion')
            deps = list()

            # Get all dependecies for ports
            for port in delPorts:
                xPathPort = self.sy.findXpathPortLeaf(port)
                print('Find dependecies for port {}'.format(port))
                # print("Generated Xpath:" + xPathPort)
                dep = self.sy.find_data_dependencies(str(xPathPort))
                if dep:
                    deps.extend(dep)
            self.logInFile('Dependencies', deps)

            # No further action with no force and deps exist
            if force == False and deps:
                return deps, False;

            # delets all deps, No topological sort is needed as of now, if deletion
            # of deps fails, return immediately
            elif deps and force:
                for dep in deps:
                    self.logInFile('Deleting', dep)
                    self.sy.delete_node(str(dep))

            # all deps are deleted now, delete all ports now
            for port in delPorts:
                xPathPort = self.sy.findXpathPort(port)
                print("Deleting Port: " + port)
                self.logInFile('Deleting Port:{}'.format(port), xPathPort)
                self.sy.delete_node(str(xPathPort))

            # Let`s Validate the tree now
            if self.validateConfigData()==False:
                return None, False

            # All great if we are here, Lets get the diff and update Config
            self.configdbJsonOut = self.sy.get_data()
            self.updateDiffConfigDB()

        except Exception as e:
            print(e)
            print("Port Deletion Failed")
            return deps, False

        return None, True

    """
    Add Ports and default config for ports to config DB, after validation of
    data tree

    PortJson: Config DB Json Part of all Ports same as PORT Table of Config DB.
    ports = list of ports
    loadDefConfig: If loadDefConfig add default config as well.

    return: Sucess: True or Failure: False
    """
    def addPorts(self, ports=list(), portJson=dict(), loadDefConfig=True):

        try:
            self.logInFile("addPorts ports:{} loadDefConfig:{}".format(ports, loadDefConfig))
            self.logInFile("addPorts Args portjson: ", portJson)

            print('\nStart Port Addition')
            # get default config if forced
            defConfig = dict()
            if loadDefConfig:
                defConfig = self.getDefaultConfig(ports)
                self.logInFile('Default Config for {}'.format(ports), \
                    defConfig, json=True)
            #prtprint(defConfig)

            # Merge PortJson and default config
            portJson.update(defConfig)
            defConfig = None
            #prtprint(portJson)

            # get the latest Data Tree, save this in input config, since this
            # is our starting point now
            self.configdbJsonIn = self.sy.get_data()

            # Get the out dict as well, if not done already
            if self.configdbJsonOut is None:
                self.configdbJsonOut = self.sy.get_data()

            # merge new config with data tree, this is json level merge
            print("Merge Port Config for {}".format(ports))
            self.mergeConfigs(self.configdbJsonOut, portJson)
            # create a tree with merged config and validate, if validation is
            # sucessful, then configdbJsonOut contains final and valid config.
            self.sy.load_data(self.configdbJsonOut)
            if self.validateConfigData()==False:
                return False

            # All great if we are here, Let`s get the diff and update COnfig
            self.updateDiffConfigDB()

        except Exception as e:
            print(e)
            print("Port Addition Failed")
            return False

        return True

    """
    Validate current Data Tree
    """
    def validateConfigData(self):

        try:
            self.sy.validate_data_tree()
        except Exception as e:
            return False

        print('Data Validation successful')
        return True

    """
    Merge second dict in first, Note both first and second dict will be changed
    First Dict will have merged part D1 + D2
    Second dict will have D2 - D1 [unique keys in D2]
    """
    def mergeConfigs(self, D1, D2):

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

            # merge rest of the keys in D1
            D1.update(D2)
        except Exce as e:
            print("Merge Config failed")
            print(e)
            raise e

        return D1

    """
    Create a defConfig for given Ports from Default Config File.
    """
    def getDefaultConfig(self, ports=list()):

        """
        create Default Config using DFS for all ports
        """
        def createDefConfig(In, Out, ports):

            found = False
            if isinstance(In, dict):
                for key in In.keys():
                    #print("key:" + key)
                    for port in ports:
                        # pattern is very specific to current primary keys in
                        # config DB, may need to be updated later.
                        pattern = '^' + port + '\|' + '|' + port + '$' + \
                            '|' + '^' + port + '$'
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
                        if createDefConfig(In[key], Out[key], ports) == False:
                            del Out[key]
                        else:
                            found = True

            elif isinstance(In, list):
                for port in ports:
                    if port in In:
                        found = True
                        Out.append(port)
                        #print("Added in list:" + port)

            else:
                # nothing for other keys
                pass

            return found

        # function code
        try:
            print("Generating default config for {}".format(ports))
            defConfigIn = readJsonFile(DEFAULT_CONFIG_DB_JSON_FILE)
            #print(defConfigIn)
            defConfigOut = dict()
            createDefConfig(defConfigIn, defConfigOut, ports)
        except Exception as e:
            print("Get Default Config Failed")
            print(e)
            raise e

        return defConfigOut

    def updateDiffConfigDB(self):

        ### Internal Functions ###
        def writeConfigDB(jDiff):
            print('Writing in Config DB')
            """
            On Sonic Switch
            """
            db_kwargs = dict(); data = dict()
            configdb = ConfigDBConnector(**db_kwargs)
            configdb.connect(False)
            deep_update(data, FormatConverter.to_deserialized(jDiff))
            data = sort_data(data)
            self.logInFile("Write in DB: Last Data", data)
            configdb.mod_config(FormatConverter.output_to_db(data))

            return

        # main code starts here
        try:
            # Get the Diff
            print('Generate Final Config to write in DB')
            configDBdiff = self.diffJson()
            #print("\n***Config Diff***\n")
            #prtprint(configDBdiff)

            # Process diff and create Config which can be updated in Config DB
            configToLoad = self.createConfigToLoad(configDBdiff, \
                self.configdbJsonIn, self.configdbJsonOut)
            #print("\n***Config To Load***\n")
            #prtprint(configToLoad)

            #Write to Config DB now
            writeConfigDB(configToLoad)

        except Exception as e:
            print("Update to Config DB Failed")
            print(e)
            raise e

        return

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
                    #print(key)
                    # make sure keys from diff are present in inp but not in outp
                    # then delete it.
                    if key in inp and key not in outp:
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

# end of config_mgmt class

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
    print('Test Run Delete Ports')
    try:
        cm = configMgmt('configDB', debug=True)
    except Exception as e:
        print(e)
        return

    delPorts = delPortDict[cmode]

    deps, ret = cm.deletePorts(delPorts=delPorts, force=True)
    if ret == False:
        print("Port Deletion Test failed")
        return None

    print("Verify Port Table in config DB for Deletion")
    portTable = getPortConfigDB()
    for port in delPorts:
        if portTable.get(port):
            print("Port {} is not deleted from config DB".format(port))
            print("Port Deletion Test Failed")
            return

    print("\n***Port Deletion Test Passed***\n")

    s = 5
    print("Wait for {} Secs".format(s))
    from time import sleep
    sleep(s)

    ports = delPortDict[nmode]
    portJson = portJsonDict[nmode].copy()

    ret = cm.addPorts(ports=ports, portJson=portJson, loadDefConfig=loadDef)
    if ret == False:
        print("Port Addition Test failed")
        return None

    # get the port list back, AddPorts may have changed them
    print("Verify Port Table in config DB for Addition")
    ports = delPortDict[nmode]
    portJson = portJsonDict[nmode]
    portTable = getPortConfigDB()
    #print(portTable)
    for port in ports:
        if portJson['PORT'][port] != portTable[port]:
            print("Port {} is not added to config DB correctly".format(port))
            print("Port Deletion Test Failed")
            return

    print("\n***Port Addition Test Passed***\n")

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
