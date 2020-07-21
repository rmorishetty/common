#!./venv/bin/python

# workflow.py
# an XML document builder for oozie


from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
from xml.dom import minidom
import sys
import string


INITIAL_ACTION_POSITION = 2  # append first action after global (0) start (1) nodes
WORKFLOW_NAMESPACE = 'uri:oozie:workflow:0.5'
SQOOP_NAMESPACE = 'uri:oozie:sqoop-action:0.2'
SHELL_NAMESPACE = 'uri:oozie:shell-action:0.2'
KILL_MESSAGE = 'Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]'

CHMOD_DIR_FILE_DEFAULT = 'true'
CHMOD_RECURSION_DEFAULT = True

#NAME_NODE_DEFAULT = config.get('NAME_NODE', 'NAME_NODE_DEFAULT')
#NAME_NODE_DEFAULT = cnf.NAME_NODE_DEFAULT
#NAME_NODE_DEFAULT = 'hdfs://h2ms01lax01us.prod.auction.local:8020'
#JOB_TRACKER_DEFAULT = config.get('JOB_TRACKER', 'JOB_TRACKER_DEFAULT')
#JOB_TRACKER_DEFAULT = cnf.JOB_TRACKER_DEFAULT
#JOB_TRACKER_DEFAULT = 'h2ms01lax01us.prod.auction.local:8050'
#USE_SYSTEM_LIBPATH_DEFAULT = 'true'
#SHARE_LIBPATH_DEFAULT = '${nameNode}/user/oozie/share/lib'


#--shared functions-------------------------------------------------------------

def xml_pretty_print(xml, raw):
    '''
    Convert etree.ElementTree objects to well-formatted xml strings.
    :param xml: An etree.ElementTree object to convert to string
    :param raw: If raw then return unformatted string, else return pretty xml string.
    :return: The ElementTree as string.
    '''
    raw_string = ElementTree.tostring(xml, 'utf-8')
    if raw == True: return raw_string
    parsed = minidom.parseString(raw_string)
    return parsed.toprettyxml(indent="    ", encoding='UTF-8')


#-------------------------------------------------------------------------------


class Workflow:
    '''
    Build Oozie workflow xml documents.
    Currently implemented sub-elements:
        - global
        - start
        - kill
        - end
        - action
    '''
    def __init__(self, name, conf_list = [], namespace=WORKFLOW_NAMESPACE):
        self.xml = Element('workflow-app')
        self.xml.set('name', name)
        self.xml.set('xmlns', namespace)
        self.global_node = self._append_global_node(conf_list)
        self.start_node = self._append_start_node()
        self.kill_node = self._append_kill_node()
        self.end_node = self._append_end_node()
        self.last_node = self.start_node
        self.action_counter = INITIAL_ACTION_POSITION

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def _append_global_node(self, conf_list = []):
        node = SubElement(self.xml,'global')
        job_tracker = SubElement(node, 'job-tracker')
        job_tracker.text = '${jobTracker}'
        name_node = SubElement(node, 'name-node')
        name_node.text = '${nameNode}'
        self.configsec(node, conf_list)
        return node

    def _append_start_node(self):
        node = SubElement(self.xml,'start')
        return node

    def _append_kill_node(self):
        node = SubElement(self.xml,'kill')
        node.set('name', 'kill')
        message = SubElement(node, 'message')
        message.text = KILL_MESSAGE
        return node

    def _append_end_node(self):
        node = SubElement(self.xml, 'end')
        node.set('name', 'end')
        self.start_node.set('to','end')
        return node

    def insert_action(self, action):
        new_name = '%s_%s' % (action.xml.get('name'), str(self.action_counter))
        action.xml.set('name', new_name)
        action.ok_node.set('to', self.end_node.get('name'))
        action.error_node.set('to', self.kill_node.get('name'))
        if self.last_node == self.start_node:
            self.last_node.set('to', action.xml.get('name'))
        else:
            self.last_node.find('ok').set('to', action.xml.get('name'))
        self.xml.insert(self.action_counter, action.xml)
        self.last_node = self.xml[self.action_counter]
        self.action_counter += 1

    def to_string(self, raw=False):
        return xml_pretty_print(self.xml, raw)

    def configsec(self, node, inp_list = []):
        self.node = node
        self.configuration = SubElement(self.node, 'configuration')
        proptext = [self.property(prop[0], prop[1]) for prop in inp_list]
        return proptext

    def property(self, key, value):
        property = SubElement(self.configuration,'property')
        name = SubElement(property,'name')
        name.text = key
        val = SubElement(property,'value')
        val.text = value


class Action:
    '''
    Build Oozie action nodes.
    Insert actions to workflow using Workflow.insert_action()
    Currently implemened actions:
        - fs (delete, mkdir, chmod, touchz)
        - sqoop (arg, file)
        - pig (script, param)
    '''
    def __init__(self):
        self.xml = Element('action')
        self.ok_node = self._append_ok_node()
        self.error_node = self._append_error_node()
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def _append_ok_node(self):
        node = SubElement(self.xml, 'ok')
        return node

    def _append_error_node(self):
        node = SubElement(self.xml, 'error')
        return node

    def to_string(self, raw=False):
        return xml_pretty_print(self.xml, raw)


class FSAction(Action):
    def __init__(self):
        Action.__init__(self)
        action_type = 'fs'
        self.xml.set('name', action_type)
        this_node = Element(action_type)
        self.xml.insert(self._position, this_node)
        self.fs = self.xml[self._position]


    def delete(self, path):
        node = SubElement(self.fs, 'delete')
        node.set('path', path)

    def mkdir(self, path):
        node = SubElement(self.fs, 'mkdir')
        node.set('path', path)

    def chmod(self,
              path,
              permission,
              dir_file=CHMOD_DIR_FILE_DEFAULT,
              recursive=CHMOD_RECURSION_DEFAULT):
        node = SubElement(self.fs, 'chmod')
        node.set('path', path)
        node.set('permissions', permission)
        node.set('dir-files', dir_file)
        if recursive:
            SubElement(node, 'recursive')

    def touchz(self, path):
        node = SubElement(self.fs, 'touchz')
        node.set('path', path)


class SqoopAction(Action):
    def __init__(self, namespace=SQOOP_NAMESPACE):
        Action.__init__(self)
        action_type = 'sqoop'
        self.xml.set('name', action_type)
        this_node = Element(action_type)
        this_node.set('xmlns', namespace)
        self.xml.insert(self._position, this_node)
        self.sqoop = self.xml[self._position]
        self._append_jt_node()
        self._append_nn_node()

    def _append_jt_node(self):
        job_tracker = SubElement(self.sqoop, 'job-tracker')
        job_tracker.text = '${jobTracker}'

    def _append_nn_node(self):
        job_tracker = SubElement(self.sqoop, 'name-node')
        job_tracker.text = '${nameNode}'

    def configsec(self, inp_list):
        self.configuration = SubElement(self.sqoop, 'configuration')
        proptext = [self.property(prop[0], prop[1]) for prop in inp_list]
        return proptext

    def property(self, key, value):
        property = SubElement(self.configuration,'property')
        name = SubElement(property,'name')
        name.text = key
        val = SubElement(property,'value')
        val.text = value

    def arg(self, inner_text):
        node = SubElement(self.sqoop, 'arg')
        node.text = inner_text

    def file(self, inner_text):
        node = SubElement(self.sqoop, 'file')
        node.text = inner_text


class ShellAction(Action):
    def __init__(self, namespace=SHELL_NAMESPACE):
        Action.__init__(self)
        action_type = 'shell'
        self.xml.set('name', action_type)
        this_node = Element(action_type)
        this_node.set('xmlns', namespace)
        self.xml.insert(self._position, this_node)
        self.shell = self.xml[self._position]
        self._append_jt_node()
        self._append_nn_node()
        #cpnode = SubElement(this_node, 'capture-output')

    def _append_jt_node(self):
        job_tracker = SubElement(self.shell, 'job-tracker')
        job_tracker.text = '${jobTracker}'

    def _append_nn_node(self):
        name_node = SubElement(self.shell, 'name-node')
        name_node.text = '${nameNode}'

    def argument(self, inner_text):
        node = SubElement(self.shell, 'argument')
        node.text = inner_text

    def env_var(self, inner_text):
        node = SubElement(self.shell, 'env-var')
        node.text = inner_text

    def file(self, inner_text):
        node = SubElement(self.shell, 'file')
        node.text = inner_text

    def execute(self, inner_text):
        node = SubElement(self.shell, 'exec')
        node.text = inner_text

    def capture_out(self):
        cpnode = SubElement(self.shell, 'capture-output')


class SubWorkflowAction(Action):
    def __init__(self):
        Action.__init__(self)
        action_type = 'sub-workflow'
        self.xml.set('name', action_type)
        this_node = Element(action_type)
        self.xml.insert(self._position, this_node)
        self.sub_workflow = self.xml[self._position]

    def app_path(self, inner_text):
        node = SubElement(self.sub_workflow, 'app-path')
        cpnode = SubElement(self.sub_workflow, 'propagate-configuration')
        node.text = inner_text

    def configsec(self, inp_list):
        self.configuration = SubElement(self.sub_workflow, 'configuration')
        proptext = [self.property(prop[0], prop[1]) for prop in inp_list]
        return proptext

    def property(self, key, value):
        property = SubElement(self.configuration,'property')
        name = SubElement(property,'name')
        name.text = key
        val = SubElement(property,'value')
        val.text = value


class PigAction(Action):
    def __init__(self):
        Action.__init__(self)
        action_type = 'pig'
        self.xml.set('name', action_type)
        this_node = Element(action_type)
        self.xml.insert(self._position, this_node)
        self.pig = self.xml[self._position]
        self._append_jt_node()
        self._append_nn_node()

    def _append_jt_node(self):
        job_tracker = SubElement(self.pig, 'job-tracker')
        job_tracker.text = '${jobTracker}'

    def _append_nn_node(self):
        job_tracker = SubElement(self.pig, 'name-node')
        job_tracker.text = '${nameNode}'

    def configsec(self, inp_list):
        self.configuration = SubElement(self.pig, 'configuration')
        proptext = [self.property(prop[0], prop[1]) for prop in inp_list]
        return proptext

    def property(self, key, value):
        property = SubElement(self.configuration,'property')
        name = SubElement(property,'name')
        name.text = key
        val = SubElement(property,'value')
        val.text = value

    def script(self, inner_text):
        node = SubElement(self.pig, 'script')
        node.text = inner_text

    def param(self, inner_text):
        node = SubElement(self.pig, 'param')
        node.text = inner_text


class Configuration:
    '''
    Build Oozie configuration xml documents.
    POST configuration as Oozie API payload using woozie.OozieClient
    '''
    def __init__(self, NAME_NODE_DEFAULT, JOB_TRACKER_DEFAULT, USE_SYSTEM_LIBPATH_DEFAULT, SHARE_LIBPATH_DEFAULT):
        self.xml = Element('configuration')
        self.NAME_NODE_DEFAULT = NAME_NODE_DEFAULT
        self.JOB_TRACKER_DEFAULT = JOB_TRACKER_DEFAULT
        self.USE_SYSTEM_LIBPATH_DEFAULT = USE_SYSTEM_LIBPATH_DEFAULT
        self.SHARE_LIBPATH_DEFAULT = SHARE_LIBPATH_DEFAULT
        default = [('nameNode', NAME_NODE_DEFAULT.replace("'","")),
                    ('jobTracker', JOB_TRACKER_DEFAULT.replace("'","")),
                    ('oozie.use.system.libpath', USE_SYSTEM_LIBPATH_DEFAULT),
                    ('ozie.libpath', SHARE_LIBPATH_DEFAULT)]
        [self.property(item[0], item[1]) for item in default]

    def __enter__(self):
        return self

    def __exit__(self, *err):
        pass

    def property(self, key, value):
        property = SubElement(self.xml,'property')
        name = SubElement(property,'name')
        name.text = key
        val = SubElement(property,'value')
        val.text = value

    def to_string(self, raw=False):
        return xml_pretty_print(self.xml, raw)



# if __name__ == '__main__':
#     workflow = Workflow(name='test_workflow')
#     fs = FSAction()
#     fs.delete('/auction/data/raw/test')
#     fs.mkdir('/auction/data/raw/test')
#     workflow.insert_action(fs)
#     sqoop = SqoopAction()
#     sqoop.arg('import')
#     sqoop.arg('--table=notification_subscriber')
#     sqoop.arg('-m=1')
#     workflow.insert_action(sqoop)
#     fs = FSAction()
#     fs.chmod('/auction/data/raw/test')
#     workflow.insert_action(fs)
#     pig = PigAction()
#     pig.script('script.pig')
#     pig.param('TARGET_FILE=path')
#     workflow.insert_action(pig)
#     fs = FSAction()
#     fs.touchz('/auction/audit/process_indicator/test.success')
#     workflow.insert_action(fs)
#     print workflow.pretty_print()
