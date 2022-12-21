import pathlib

from ansible.module_utils.basic import *

def main():
    module_args = dict(
        splex_port=dict(type='str', required=True),
        source_db_name=dict(type='str', required=True),
        source_schema_name=dict(type='str', required=True),
        source_host_name=dict(type='str', required=True),
        target_db_name=dict(type='str', required=True),
        target_schema_name=dict(type='str', required=True),
        target_host_name=dict(type='str', required=True),
        db_type=dict(type='str', required=True),
        tablelist=dict(type='dict', required=True),
        splex_comment=dict(type='str', required=False)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False
    )
    result = dict(
        status='successed',
        err_message=''
    )
    port = str(module.params['splex_port'])
    source_db_name = module.params['source_db_name']
    target_db_name = module.params['target_db_name']
    source_schema_name = module.params['source_schema_name']
    target_schema_name = module.params['target_schema_name']
    source_host_name = module.params['source_host_name']
    target_host_name = module.params['target_host_name']
    db_type = module.params['db_type']
    tablelist = module.params['tablelist']
    splex_comment = module.params['splex_comment']

    # check param
    res = check_param(db_type,tablelist)
    if res['status'] != "success":
        result['status'] = 'failed'
        err_message = "Error! %s" %(res['errormsg'])
        module.exit_json(changed=True, meta=result, msg=err_message)

    cmd = " ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'| cut -d '/' -f  1-3 " % (
        port)
    rc, out, err = module.run_command(cmd, use_unsafe_shell=True)
    # check port status
    if len(str(out))==0:
        result['status'] = 'failed'
        err_message = "Error! Shareplex port %s is not running." %(port)
        module.exit_json(changed=True, meta=result,msg=err_message)

    # get release number from splex_comment
    release_number = ""
    if splex_comment:
        matchObj = re.search('release[ ]?[\d]+', splex_comment)
        if matchObj:
            release_number = str(matchObj.group(0)).split("release")[1].strip()

    shareplex_bin_dir = "%s/bin" % (str(out).rstrip())
    cmd = "source %s/.profile_u%s ; cd %s; cat .profile_u%s | grep SP_SYS_VARDIR" %(shareplex_bin_dir,port,shareplex_bin_dir,port)
    rc, out0, err = module.run_command(cmd, use_unsafe_shell=True)
    shareplex_var_dir = str(out0.split(";")[0]).split("=")[1]
    result['shareplex_bin_dir'] = shareplex_bin_dir
    result['shareplex_var_dir'] = shareplex_var_dir
    cmd = "cat %s/data/statusdb | grep \"active from\"| awk -F\\\" '{print $2}' " % (shareplex_var_dir)
    rc, out1, err = module.run_command(cmd, use_unsafe_shell=True)
    if rc != 0:
        result['status'] = 'failed'
        module.exit_json(changed=True, meta=result,msg=err)
    else:
        #  if active file exist and not configured before, then copy a new active file, otherwise create a new active file
        original_active = False
        original_active_config = ""
        if len(str(out1).rstrip()) > 0:
            original_active = True
            original_active_config_name = str(out1).rstrip()
            original_active_config = '%s/config/%s' % (shareplex_var_dir, original_active_config_name)
            result['original_active_config'] = original_active_config_name
        new_active_config = '%s/config/auto.%s.%s2%s.%s.cfg' % (
        shareplex_var_dir, release_number, source_db_name, target_db_name,
        datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y%m%d.%H%M%S"))
        splex_comment = "#%s" % (splex_comment)

        skip_add_table = {}
        # 1. add or remove tabel list
        if original_active:
            # 1.1) find queue name of monitor table splex_monitor_adb
            cmd = "cat %s |grep %s.splex_monitor_adb |grep @%s.%s |grep -v \"^#\" " %(original_active_config,source_schema_name,db_type,target_db_name)
            rc, out_monitor_tab, err = module.run_command(cmd, use_unsafe_shell=True)
            rounting = out_monitor_tab.split(" ")[-1].strip()
            index_start = rounting.find(":")
            index_end = rounting.find("*")
            monitor_tab_qname = ""
            if index_start > 0 and index_end:
                monitor_tab_qname = rounting[index_start + 1:index_end]

            # 1.2) parse active config file and get all table list
            cmd = "cat %s |grep -v ^# | grep -v ^\"datasource\"" %(original_active_config)
            rc, out_tabs, err = module.run_command(cmd, use_unsafe_shell=True)
            all_config_table = parse_config_file(out_tabs)

            # 1.3) Copy one by one to the new confile file, if the record needs to be changed or removed, comment it first
            annotations = annotation_tab_name_list(tablelist,all_config_table,source_schema_name,target_db_name,source_host_name,target_host_name)
            # result['annotations'] = annotations
            annotations_arr = '''(%s)''' % (' '.join('"{0}"'.format(x) for x in annotations))
            annotations_arr = re.sub(r'\\', '', annotations_arr)

            cmd = '''
            original_active_config="%s"
            table_name_arr=%s
            new_active_config="%s"
            splex_comment="%s"

            cat $original_active_config | while read line
            do
                exist=false
                for value in ${table_name_arr[@]}
                do
                    if [[ $line == *$value* ]] && [[ $line =~ ^[^#.*] ]];then
                        exist=true
                    fi
                done
                if [ "$exist" = true ] ; then
                    echo $splex_comment >> $new_active_config
                    echo "#"$line >> $new_active_config
                else
                    echo $line >> $new_active_config
                fi
            done
            ''' % (original_active_config, annotations_arr, new_active_config,splex_comment)

            module.run_command(cmd, use_unsafe_shell=True)
            # 1.3) add tablelist into the end of new config file
            to_add_new_cfg,skip_add_table = tablelist2cfgfile(tablelist,monitor_tab_qname, db_type, source_db_name, target_db_name, source_schema_name,
                                               target_schema_name, source_host_name, target_host_name,splex_comment,all_config_table)
            cmd = " echo -e \"{0}\">>{1}".format(to_add_new_cfg, new_active_config)
            module.run_command(cmd, use_unsafe_shell=True)
        else:
            if "add_table" in tablelist and len(tablelist['add_table'].keys())>0:
                # 1.1) create a new config file
                pathlib.Path(new_active_config).touch()
                # 1.2) write table list into the new config file
                to_add_new_cfg_head = 'datasource:%s.%s\n' % (db_type,source_db_name)
                to_add_new_cfg_tail,skip_add_table = tablelist2cfgfile(tablelist,"",db_type,source_db_name,target_db_name,source_schema_name,target_schema_name,source_host_name,target_host_name,splex_comment,"")
                to_add_new_cfg = to_add_new_cfg_head + to_add_new_cfg_tail
                cmd = " echo -e \"{0}\">>{1}".format(to_add_new_cfg, new_active_config)
                module.run_command(cmd, use_unsafe_shell=True)
            else:
                result['status'] = 'failed'
                err_message = "Error! No active file found,cannot remove table."
                module.exit_json(changed=True, meta=result,msg = err_message)
        # 3. verify config
        verify_flag = False
        new_active_config_filename = new_active_config.split("%s/config/" % (shareplex_var_dir))[1]
        cmd = """
                source %s/.profile_u%s; cd %s;
                ./sp_ctrl << EOF
                verify config %s
                show
                EOF
        """ % (shareplex_bin_dir, port, shareplex_bin_dir, new_active_config_filename)
        rc, out2, err = module.run_command(cmd, use_unsafe_shell=True)
        verify_message = "Config %s is valid" % (new_active_config_filename)
        if verify_message in out2:
            verify_flag = True
        else:
            result['status'] = 'failed'
            err_message = "Error! verify new config file %s" % (new_active_config_filename)
            module.exit_json(changed=True, meta=result,msg= err_message)
        result['verify_new_config_flag'] = verify_flag
        #4. if verify success, then activate new config file
        post_active_config = ""
        if verify_flag:
            if result['verify_new_config_flag'] == True:
                cmd = """
                                source %s/.profile_u%s; cd %s;
                                ./sp_ctrl << EOF
                                activate config %s
                                show
                                EOF
                        """ % (shareplex_bin_dir, port, shareplex_bin_dir, new_active_config_filename)
                module.run_command(cmd, use_unsafe_shell=True)
            # find current activate config file
            cmd = "cat %s/data/statusdb | grep \"active from\"| awk -F\\\" '{print $2}' " % (shareplex_var_dir)
            rc, out3, err = module.run_command(cmd, use_unsafe_shell=True)
            if len(str(out3).rstrip()) > 0:
                post_active_config = '%s/config/%s' % (shareplex_var_dir, str(out3).rstrip())
        # 5. post verify
        activate_new_config_flag = False
        if new_active_config == post_active_config:
            activate_new_config_flag = True
            result['active_config'] = new_active_config.split("/")[-1]
        result['activate_new_config_flag'] = activate_new_config_flag


    module.exit_json(changed=True, meta=result)

def parse_config_file(config_content):
    """
        parse shareplex config file and get all replication table

        @params:
            :param config_content        :  shareplex config file content

        @return: {
                "<table_name>":
                {
                    "partition_v": "",
                    "base_key": "",
                    "source_host_name": "",
                    "source_schema_name": "",
                    "table_name": "",
                    "target_db_name": "",
                    "target_host_name": ""
                }
            }
        """
    all_config_table = {}
    for line in config_content.splitlines():
        line = line.strip()
        if line:
            line_a = line.split()
            item = {}
            source = line_a[0]
            source_schema_name = source.split(".")[0]
            table_name = source.split(".")[1]
            routing = line_a[-1]
            target_db_name = str(routing.split("@")[-1]).split(".")[-1]
            source_host_name = ""
            partition_v = ""
            base_key = ""
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
            all_config_table[table_name] = item
    return all_config_table

def tablelist2cfgfile(tablelist,monitor_tab_qname,db_type,source_db_name,target_db_name,source_schema_name,target_schema_name,source_host_name,target_host_name,splex_comment,all_config_table):
    """
    Convert the table that needs to be added to the content appended to the end of the config file
    """
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
                partition_v = str(to_do_tab['partition_v'] or "")
                base_key = str(to_do_tab['base_key'] or "")
                config_tab = all_config_table[tab_name]
                if source_schema_name.lower() == config_tab['source_schema_name'] \
                        and target_db_name.lower() == config_tab['target_db_name'].lower() \
                        and source_host_name == config_tab['source_host_name'] \
                        and target_host_name == config_tab['target_host_name'] \
                        and partition_v == config_tab['partition_v'] \
                        and base_key == config_tab['base_key']:
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
                if base_key:
                    cfg_item += "%s " % (base_key)
                if partition_v:
                    cfg_item += "%s " % (partition_v)
                cfg_item += "%s " % (target)
                cfg_item += "%s\n" % (routing)
                cfg_file += "%s" % (cfg_item)
    if cfg_file.strip():
        cfg_file = "\n" + splex_comment + "\n" + cfg_file
    return cfg_file,skip_table


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

def check_param(db_type,tablelist):
    res = {"status": "success", "errormsg": ""}
    if "add_table" not in tablelist and "remove_table" not in tablelist:
        res['status'] = "fail"
        res['errormsg'] = "Parameter tablelist is invalid, please check."
        return res
    if db_type not in ["r","o"]:
        res['status'] = "fail"
        res['errormsg'] = "Parameter db_type is invalid, please check."
        return res
    return res

if __name__ == "__main__":
    main()