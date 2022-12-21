import re

import prettytable as pt
from prettytable import from_html


def main():

    qstatus = '''

Queues Statistics for amdbpgtest
  Name:  amdbpgtest02 (r.pggvweb-r.pgguweb) (Post queue)
    Number of messages:          0 (Age         0 min; Size          1 mb)
    Backlog (messages):          0 (Age         0 min)

  Name:  r.pgguweb (Capture queue)
    Number of messages:       8696 (Age         0 min; Size          6 mb)
    Backlog (messages):          0 (Age         0 min)

  Name:  amdbpgtest (Export queue)
    Number of messages:       2463 (Age         0 min; Size          5 mb)
    Backlog (messages):          0 (Age         0 min)
    '''

    parts = qstatus.split("\n\n\n")
    queues_detail = ""
    for part in parts:
        part = part.lstrip()
        if part.startswith("Queues Statistics for"):
            queues_detail = str(part).rstrip()
            break
    print(queues_detail)

def todo_src_tab_name_list(tablelist):
    todo_list = []
    if "add_table" in tablelist:
        for tab_name in tablelist['add_table'].keys():
            if tab_name not in todo_list:
                todo_list.append(tab_name)
    if "remove_table" in tablelist:
        for tab_name in tablelist['remove_table'].keys():
            if tab_name not in todo_list:
                todo_list.append(tab_name)
    return todo_list

def tablelist2cfgfile(tablelist,db_type,source_db_name,target_db_name,source_schema_name,target_schema_name,source_host_name,target_host_name):
    cfg_file = ""
    if ".webex.com" in source_host_name.lower():
        source_host_name = source_host_name.lower().split(".webex.com")[0]
    if ".webex.com" in target_host_name.lower():
        target_host_name = target_host_name.lower().split(".webex.com")[0]
    if "add_table" in tablelist:
        add_tables = tablelist['add_table']
        for tab_name in add_tables.keys():
            table_dict = add_tables[tab_name]
            source_key = table_dict['source_key']
            patition_h = table_dict['patition_h']
            patition_v = table_dict['patition_v']
            source = "%s.%s" %(source_schema_name,tab_name)
            target = "%s.%s" % (target_schema_name, tab_name)
            trim_src_db = str(source_db_name).strip().lower()
            trim_tgt_db = str(target_db_name).strip().lower()
            if trim_src_db.startswith("pg"):
                trim_src_db = trim_src_db.split("pg")[1]
            if trim_src_db.startswith("rac"):
                trim_src_db = trim_src_db.split("rac")[1]
            if trim_tgt_db.startswith("pg"):
                trim_tgt_db = trim_tgt_db.split("pg")[1]
            if trim_tgt_db.startswith("rac"):
                trim_tgt_db = trim_tgt_db.split("rac")[1]
            if "o" == db_type:
                trim_src_db = trim_src_db.upper()
                trim_tgt_db = trim_tgt_db.upper()
            routing = "%s:%s2%s*%s@%s.%s" %(source_host_name,trim_src_db,trim_tgt_db,target_host_name,db_type,target_db_name)
            # oracle example: sjdbormt066-vip:NewAudit_GSBAudit*txdbormt098-vip@o.AUDITGSB_SPLEX
            # pg example: amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
            cfg_item = "%s " % (source)
            if patition_v:
                cfg_item += "%s " % (patition_v)
            if source_key:
                cfg_item += "%s" % (source_key)
            cfg_item += "%s " % (target)
            cfg_item += "%s\n" % (routing)
            cfg_file += "%s" % (cfg_item)
    else:
        return cfg_file
    return cfg_file


def parse_list_config():
    config_files_content = '''
    
File   Name                                         State       Datasource
--------------------------------------------------  ----------  ---------------
auto.20221205_045746.cfg                            Inactive    r.pgguweb
Last Modified At: 05-Dec-22 04:57    Size: 1432

auto.20221205_045636.cfg                            Inactive    r.pgguweb
Last Modified At: 05-Dec-22 04:56    Size: 1427

auto.20221205_045342.cfg                            Inactive    r.pgguweb
Last Modified At: 05-Dec-22 04:53    Size: 1423

test_copy.cfg                                       Inactive    r.pgguweb
Last Modified At: 01-Dec-22 02:53    Size: 283

test.cfg                                            Inactive    r.pgguweb
Last Modified At: 28-Oct-22 08:04    Size: 1498

sp_60064_config_post_0926.cfg_new                   Inactive    r.pgguweb
Last Modified At: 24-Oct-22 01:43    Size: 1197

test_discard.cfg                                    Active      r.pgguweb
Last Modified At: 05-Dec-22 01:10    Size: 1372    Internal Name: .conf.89
    '''
    files = {}

    config_files = {}
    for line in config_files_content.splitlines():
        if str(line).strip() and (str(line).strip().find("Inactive")>0 or str(line).strip().find("Active")>0):
            filename = str(line).strip().split()[0]
            status = str(line).strip().split()[1]
            datasource = str(line).strip().split()[2]
            item = {}
            item['status'] = status
            item['datasource'] = datasource
            config_files[filename] = item
    return config_files

def parse_config_file():
#     config_content = '''
# splex60064.splex_monitor_adb splex60064.splex_monitor_adb amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
#
# test.wbxmmconference       test.wbxmmconference       10.248.169.135@r.pggvweb
# test.wbxguest                         test.wbxguest                         10.248.169.135@r.pggvweb
# test.wbxmmconfparam                   test.wbxmmconfparam                   10.248.169.135@r.pggvweb
# splex60064.DEMO_SRC splex60064.DEMO_SRC 				  10.248.169.135@r.pggvweb
# splex60064.splex_monitor_adb (direction,src_host,src_db,port_number,logtime) splex60064.splex_monitor_adb amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
# xxrpth.XXRPT_HGSMEETINGUSERREPORT !key(HGSSITEID,CONFID,OBJID) (CONFID,CONFKEY,CONFNAME,CONFTYPE,USERTYPE,STARTTIME,ENDTIME,DURATION,UID_,GID,WEBEXID,USERNAME,USEREMAIL,IPADDRESS,SITEID,SITENAME,DIALIN1,DIALIN2,DIALOUT1,DIALOUT2,VOIP,HGSSITEID,MEETINGTYPE,TIMEZONE,OBJID,CLIENTAGENT,MTG_STARTTIME,MTG_ENDTIME,MTG_TIMEZONE,REGISTERED,INVITED,REG_COMPANY,REG_TITLE,REG_PHONE,REG_ADDRESS1,REG_ADDRESS2,REG_CITY,REG_STATE,REG_COUNTRY,REG_ZIP,NON_BILLABLE,ATTENDANTID,ATT_FIRSTNAME,ATT_LASTNAME,SERVICE_IDENTIFIER,USER_IDENTIFIER,PURCHASE_IDENTIFIER,CONSUMPTION_DATE,SUBSCRIPTION_CODE,BOSS_CONTRACTID,RBE_STATUS,RBE_TIMESTAMP,ISINTERNAL,PROSUMER,INATTENTIVEMINUTES,MEETINGLANGUAGEID,REGIONID,ACCOUNTID,SERVICECODE,SUBSCRIPTIONCODE,BILLINGACCOUNTID,MEETINGNODEFLAG,ATTENDEETAG,PRIVATE_IPADDRESS,CBUSERID,TPDIALIN,HOSTID,CREATEDATE,LASTMODIFIEDDATE) xxrpth.XXRPT_HGSMEETINGUSERREPORT sjdbormt093-vip:med2sjteo0n*sjdbormt011-vip@o.RACSJRPT_SPLEX
#     '''
    config_content = '''
    datasource:r.pgguweb
#splex60064.splex_monitor_adb          splex60064.splex_monitor_adb       10.248.169.135@r.pggvweb
splex60064.splex_monitor_adb splex60064.splex_monitor_adb amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
#splex60064.shareplexdatarep          splex60064.shareplexdatarep       10.248.169.135@r.pggvweb
#"postgres"."pgbench_accounts"       "postgres"."pgbench_accounts"       10.248.169.135@r.pggvweb
#"postgres"."pgbench_branches"       "postgres"."pgbench_branches"       10.248.169.135@r.pggvweb
#"postgres"."pgbench_history"        "postgres"."pgbench_history"        10.248.169.135@r.pggvweb
#"postgres"."pgbench_tellers"        "postgres"."pgbench_tellers"        10.248.169.135@r.pggvweb

#test.wbxmtginstanceparticiprelation   test.wbxmtginstanceparticiprelation   10.248.169.135@r.pggvweb
#test.wbxmeetinginstanceinvitee        test.wbxmeetinginstanceinvitee        10.248.169.135@r.pggvweb
#test.wbxmeetinginstancemetadata       test.wbxmeetinginstancemetadata       10.248.169.135@r.pggvweb
test.wbxmmconference       test.wbxmmconference       10.248.169.135@r.pggvweb
#test.wbxguest                         test.wbxguest                         10.248.169.135@r.pggvweb
#test.wbxmmconfparam                   test.wbxmmconfparam                   10.248.169.135@r.pggvweb
test.wbxguest                         test.wbxguest                         amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
test.wbxmmconfparam                   test.wbxmmconfparam                   amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
splex60064.DEMO_SRC splex60064.DEMO_SRC                   10.248.169.135@r.pggvweb
    
    '''
    all_config_table ={}
    for line in config_content.splitlines():
        line = line.strip()
        if line:
            line_a = line.split()
            print()
            print(line_a)
            item = {}
            source = line_a[0]
            source_schema_name = source.split(".")[0]
            table_name = source.split(".")[1]
            routing = line_a[-1]
            target_db_name = str(routing.split("@")[-1]).split(".")[-1]
            source_host_name = ""
            partition_v = ""
            base_key= ""
            if "key" in line_a[1]:
                base_key = line_a[1]
            if base_key and "(" in line_a[2]:
                partition_v = line_a[2]
            if not base_key and "(" in line_a[1]:
                partition_v = line_a[1]
            if ":" in str(routing.split("@")[0]):
                source_host_name = str(routing.split("@")[0]).split(":")[0]
            target_host_name = str(routing.split("@")[0]).split(":")[-1].split("*")[-1]
            item['source_schema_name'] = source_schema_name
            item['table_name'] = table_name
            item['target_db_name'] = target_db_name
            item['source_host_name'] = source_host_name
            item['target_host_name'] = target_host_name
            item['partition_v'] = partition_v
            item['base_key'] = base_key
            print(item)
            all_config_table[table_name] = item
    return all_config_table

def annotation_tab_name_list(tablelist,all_config_table,source_schema_name,target_db_name,source_host_name,target_host_name):
    """
            Get the table that needs to be annotated
            1. For add-table, if the table is different from that in the configuration file , then annotation old record
            2. For remove-table, if the table is same from that in the configuration file , then annotation old record

            @params:
                :param tablelist                :  ansible module wbx_splex_table param tablelist
                :param all_config_table         :  return of function parse_config_file
                :param source_schema_name       :  shareplex source_schema_name
                :param target_db_name           :  shareplex target_db_name
                :param source_host_name         :  shareplex source_host_name
                :param target_host_name         :  shareplex target_host_name

            @return: List of table names that need annotation
    """
    annotation_list = []
    key = ["add_table","remove_table"]
    for key_name in key:
        if key_name in tablelist:
            if ".webex.com" in source_host_name.lower():
                source_host_name = source_host_name.lower().split(".webex.com")[0]
            if ".webex.com" in target_host_name.lower():
                target_host_name = target_host_name.lower().split(".webex.com")[0]
            for tab_name in tablelist[key_name].keys():
                is_same = False
                if tab_name in all_config_table.keys():
                    to_do_tab = tablelist[key_name][tab_name]
                    partition_v = str(to_do_tab['partition_v'] or "")
                    base_key = str(to_do_tab['base_key'] or "")
                    config_tab = all_config_table[tab_name]
                    if "add_table"==key_name:
                        if source_schema_name.lower() == config_tab['source_schema_name'] \
                                and target_db_name.lower() == config_tab['target_db_name'].lower() \
                                and source_host_name == config_tab['source_host_name'] \
                                and target_host_name == config_tab['target_host_name'] \
                                and partition_v == config_tab['partition_v'] \
                                and base_key == config_tab['base_key']:
                            is_same = True
                    elif "remove_table"==key_name:
                        if source_schema_name.lower() == config_tab['source_schema_name'] \
                                and target_db_name.lower() == config_tab['target_db_name'].lower():
                            is_same = True
                if "add_table"==key_name and not is_same and tab_name not in annotation_list:
                    annotation_list.append(str(tab_name).strip().lower())
                if "remove_table"==key_name and is_same and tab_name not in annotation_list:
                    annotation_list.append(str(tab_name).strip().lower())
    return annotation_list

def tablelist2cfgfile():
    monitor_tab_qname =  "guweb2gvweb"
    db_type ="r"
    source_db_name = "pgguweb"
    target_db_name = "pggvweb"
    source_schema_name = "splex60064"
    target_schema_name = "splex60064"
    source_host_name = "amdbpgtest"
    target_host_name= "amdbpgtest02"
    splex_comment ="####Modified by DA in release 50050"
    all_config_table = {
                "DEMO_SRC": {
                    "partition_v": "",
                    "source_host_name": "",
                    "source_schema_name": "splex60064",
                    "table_name": "DEMO_SRC",
                    "target_db_name": "pggvweb",
                    "target_host_name": "10.248.169.135"
                },
                "splex_monitor_adb": {
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "splex60064",
                    "table_name": "splex_monitor_adb",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                },
                "wbxguest": {
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "test",
                    "table_name": "wbxguest",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                },
                "wbxmmconference": {
                    "partition_v": "",
                    "source_host_name": "",
                    "source_schema_name": "test",
                    "table_name": "wbxmmconference",
                    "target_db_name": "pggvweb",
                    "target_host_name": "10.248.169.135"
                },
                "wbxmmconfparam": {
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "test",
                    "table_name": "wbxmmconfparam",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                }
            }
    tablelist= {
          "add_table":
          {
              "splex_monitor_adb":
                {
                  "base_key": "",
                  "partition_h": "",
                  "partition_v": "(direction,src_host,src_db,port_number,logtime)"
                },
              "splex_monitor_test_1":
                {
                  "base_key": "",
                  "partition_h": "",
                  "partition_v": ""
                }
          },
          "remove_table":
          {

          }
      }

    cfg_file = ""

    skip_table = {}
    if ".webex.com" in source_host_name.lower():
        source_host_name = source_host_name.lower().split(".webex.com")[0]
    if ".webex.com" in target_host_name.lower():
        target_host_name = target_host_name.lower().split(".webex.com")[0]
    if "add_table" in tablelist:
        add_tables = tablelist['add_table']
        for tab_name in add_tables.keys():
            # Determine whether the added table already exists in the config file, if it exists, skip it
            is_exist = False
            if tab_name in all_config_table.keys():
                to_do_tab = add_tables[tab_name]
                partition_v = to_do_tab['partition_v']
                config_tab = all_config_table[tab_name]
                if source_schema_name.lower() == config_tab['source_schema_name'] \
                        and target_db_name.lower() == config_tab['target_db_name'].lower() \
                        and source_host_name == config_tab['source_host_name'] \
                        and target_host_name == config_tab['target_host_name'] \
                        and partition_v == config_tab['partition_v']:
                    is_exist = True
            if is_exist:
                skip_table[tab_name] = add_tables[tab_name]
            else:
                table_dict = add_tables[tab_name]
                base_key = table_dict['base_key']
                partition_h = table_dict['partition_h']
                partition_v = table_dict['partition_v']
                source = "%s.%s" % (source_schema_name, tab_name)
                target = "%s.%s" % (target_schema_name, tab_name)
                trim_src_db = str(source_db_name).strip().lower()
                trim_tgt_db = str(target_db_name).strip().lower()
                if trim_src_db.startswith("pg"):
                    trim_src_db = trim_src_db.split("pg")[1]
                if trim_src_db.startswith("rac"):
                    trim_src_db = trim_src_db.split("rac")[1]
                if trim_tgt_db.startswith("pg"):
                    trim_tgt_db = trim_tgt_db.split("pg")[1]
                if trim_tgt_db.startswith("rac"):
                    trim_tgt_db = trim_tgt_db.split("rac")[1]
                if "o" == db_type:
                    trim_src_db = trim_src_db.upper()
                    trim_tgt_db = trim_tgt_db.upper()
                queue_name = "%s2%s" % (trim_src_db, trim_tgt_db)
                if monitor_tab_qname:
                    queue_name = monitor_tab_qname
                routing = "%s:%s*%s@%s.%s" % (source_host_name, queue_name, target_host_name, db_type, target_db_name)
                # oracle example: sjdbormt066-vip:NewAudit_GSBAudit*txdbormt098-vip@o.AUDITGSB_SPLEX
                # pg example: amdbpgtest:guweb2gvweb*amdbpgtest02@r.pggvweb
                cfg_item = "%s " % (source)
                if partition_v:
                    cfg_item += "%s " % (partition_v)
                if base_key:
                    cfg_item += "%s" % (base_key)
                cfg_item += "%s " % (target)
                cfg_item += "%s\n" % (routing)
                cfg_file += "%s" % (cfg_item)
    cfg_file = "\n%s\n%s\n" %(splex_comment,cfg_file)
    return cfg_file,skip_table

def hit_process(lstatus):
    process_type = "post"
    db_type ="r"
    source_db_name = "stdmhsdb"
    target_db_name = "stdgmhsdb"
    source_host_name = "cfe1pgsql113"
    target_host_name = "cfe1pgsql114"
    found_process = []
    if process_type in ["export","post"]:
        for process in lstatus:
            data_host = ""
            if process_type =="post":
                data_host = "%s.%s-%s.%s" % (db_type,source_db_name, db_type,target_db_name)
            if process_type =="export":
                data_host = "%s" % (target_host_name)
            if process_type.lower() == process['Process'].lower() and process['Data/Host'] and data_host in process['Data/Host']:
                found_process.append(process)
    else:
        for process in lstatus:
            if process_type.lower() == process['Process'].lower():
                found_process.append(process)
    return found_process

def parser_lstatus():
    lstatus = '''
    Detailed Status for cfe1pgsql113
Process          State                             PID     Running   Since
---------------  ------------------------------  --------  --------------------
Cop              Running                           211608  03-Dec-22 07:51:55
Import           Running                           212193  03-Dec-22 07:54:20
  Data/Host:   cfe1pgsql114
  Queue Name:  cfe1pgsql114
Post             Running                           212194  03-Dec-22 07:54:20
  Data/Host:   r.stdgmhsdb-r.stdmhsdb
  Queue Name:  cfe1pgsql114
Capture          Running                           482277  08-Dec-22 01:26:42
  Data/Host:   r.stdmhsdb
Read             Running                           482304  08-Dec-22 01:26:43
  Data/Host:   r.stdmhsdb
Export           Running                           482305  08-Dec-22 01:26:43
  Data/Host:   0xc0a8be27
  Queue Name:  cfe1pgsql113
Export           Running                           482306  08-Dec-22 01:26:43
  Data/Host:   0xc0a8be27
  Queue Name:  stdmhsdb2stdgmhsdb
Cmd & Ctrl       Running                          1769027  09-Dec-22 05:34:29
  Data/Host:   cfe1pgsql113.dmz.webex.com


Queues:
 Type    # Msgs   Size (Mb)  Age (mn) Oldest Msg Time    Newest Msg Time
------- --------- ---------- -------- ------------------ ------------------
Post            0          1        0 03-Dec-22 07:54:20 03-Dec-22 07:54:20
  Queue Name:       cfe1pgsql114
  DataSrc-DataDst:  r.stdgmhsdb-r.stdmhsdb
Capture         0          0        0 09-Dec-22 01:50:46 09-Dec-22 01:50:46
  Queue Name:       r.stdmhsdb
Export          0          2        0 09-Dec-22 01:50:46 09-Dec-22 01:50:46
  Queue Name:       cfe1pgsql113
Export          0          2        0 09-Dec-22 01:50:46 09-Dec-22 01:50:46
  Queue Name:       stdmhsdb2stdgmhsdb

System is used as a source machine
Replication Configurations:

  Replication active from "splex.50008.stdmhsdb2stdgmhsdb.20221209.015043.cfg", actid 69, for database r.stdmhsdb since 09-Dec-22 01:50:46
    '''
    parts = lstatus.split("\n\n\n")
    queues_detail = ""
    for part in parts:
        part = part.lstrip()
        if part.startswith("Detailed Status for"):
            queues_detail = str(part).rstrip()
            break
    queues_detail = str(queues_detail).replace("Cop", "||Cop")
    queues_detail = str(queues_detail).replace("Capture", "||Capture")
    queues_detail = str(queues_detail).replace("Import", "||Import")
    queues_detail = str(queues_detail).replace("Post", "||Post")
    queues_detail = str(queues_detail).replace("Read", "||Read")
    queues_detail = str(queues_detail).replace("Export", "||Export")
    queues_detail = str(queues_detail).replace("Cmd & Ctrl", "||Cmd & Ctrl")

    process_arr = [x for x in queues_detail.split("||") if x != '' and "Detailed" not in x]
    process_list = []
    process_name_list = ["Cop", "Capture", "Import", "Post", "Read", "Export", "Cmd & Ctrl"]
    for item in process_arr:
        process = {}
        for line in item.split("\n"):
            line = line.strip()
            if line:
                if any(element in line for element in process_name_list):
                    pro_arr = [x for x in line.split("  ") if x != '']
                    process['Process'] = pro_arr[0]
                    process['State'] = pro_arr[1]
                else:
                    key = str(line.split(":")[0]).strip()
                    value = str(line.split(":")[1]).strip()
                    process[key] = value
        process_list.append(process)
    return process_list

if __name__ == "__main__":
    # lstatus = parser_lstatus()
    # print(lstatus)
    # hit = hit_process(lstatus)
    # print(hit)
    tablelist = {
        "remove_table": {
            "wbxmmconference": {
                "base_key": "",
                "partition_h": "",
                "partition_v": ""
            }
        }}
    all_config_table ={
                "DEMO_SRC": {
                    "base_key": "",
                    "partition_v": "",
                    "source_host_name": "",
                    "source_schema_name": "splex60064",
                    "table_name": "DEMO_SRC",
                    "target_db_name": "pggvweb",
                    "target_host_name": "10.248.169.135"
                },
                "splex_monitor_adb": {
                    "base_key": "",
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "splex60064",
                    "table_name": "splex_monitor_adb",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                },
                "wbxguest": {
                    "base_key": "",
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "test",
                    "table_name": "wbxguest",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                },
                "wbxmmconference": {
                    "base_key": "",
                    "partition_v": "",
                    "source_host_name": "",
                    "source_schema_name": "test",
                    "table_name": "wbxmmconference",
                    "target_db_name": "pggvweb",
                    "target_host_name": "10.248.169.135"
                },
                "wbxmmconfparam": {
                    "base_key": "",
                    "partition_v": "",
                    "source_host_name": "amdbpgtest",
                    "source_schema_name": "test",
                    "table_name": "wbxmmconfparam",
                    "target_db_name": "pggvweb",
                    "target_host_name": "amdbpgtest02"
                }
            }
    source_schema_name = "test"
    target_db_name = "pggvweb"
    source_host_name = "amdbpgtest.webex.com"
    target_host_name = "amdbpgtest02.webex.com"
    res = annotation_tab_name_list(tablelist,all_config_table,source_schema_name,target_db_name,source_host_name,target_host_name)
    print(res)