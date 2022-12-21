import time

from ansible.module_utils.basic import AnsibleModule

def parser_lstatus(lstatus):
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

    process_arr = [x for x in queues_detail.split("||") if x!='' and "Detailed" not in x]
    process_list = []
    process_name_list = ["Cop","Capture","Import","Post","Read","Export","Cmd & Ctrl"]
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

def hit_process(lstatus,process_type,db_type, source_db_name, target_db_name,source_host_name,target_host_name):
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

def main():
    module_args = dict(
        port=dict(type='str', required=True),
        state=dict(type='str', required=True),
        process_type=dict(type='str', required=True),
        db_type=dict(type='str', required=True),
        source_db_name=dict(type='str', required=True),
        target_db_name=dict(type='str', required=True),
        source_host_name=dict(type='str', required=True),
        target_host_name=dict(type='str', required=True),
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False
    )
    result = dict(
        status='successed',
        err_message=''
    )

    port = module.params['port']
    state = module.params['state']
    process_type = module.params['process_type']
    source_db_name = module.params['source_db_name']
    target_db_name = module.params['target_db_name']
    source_host_name = module.params['source_host_name']
    target_host_name = module.params['target_host_name']
    db_type = module.params['db_type']
    result[
        'pre_cmd'] = "state=%s, process_type=%s, port=%s, source_db_name=%s, target_db_name=%s, source_host_name=%s,target_host_name=%s " % (
    state, process_type, port, source_db_name, target_db_name, source_host_name, target_host_name)
    # 1. check parameter
    if state not in ["start","stop"]:
        result['status'] = 'failed'
        err_message = "Error! state must in [ start, stop ]"
        module.exit_json(changed=True, meta=result, msg=err_message)
    elif process_type not in ["capure","post","read","import","export"]:
        result['status'] = 'failed'
        err_message = "Error! process type must in [ capure, post, read, import, export ]"
        module.exit_json(changed=True, meta=result, msg=err_message)
    else:
        # 2. find shareplex bin dir for specific port
        cmd = " ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'| cut -d '/' -f  1-3 " % (port)
        rc, out, err = module.run_command(cmd, use_unsafe_shell=True)
        shareplex_bin_dir = "%s/bin" % (str(out).rstrip())
        result['shareplex_bin_dir'] = shareplex_bin_dir
        # 3. parser lstatus of shareplex process
        cmd = """
                source %s/.profile_u%s; cd %s;
                ./sp_ctrl << EOF
                lstatus
                EOF
                """ % (shareplex_bin_dir, port, shareplex_bin_dir)
        rc, lstatus, err = module.run_command(cmd, use_unsafe_shell=True)
        lstatus = parser_lstatus(lstatus)
        # 4. start/stop process
        # get queue name of shareplex process
        found_process = hit_process(lstatus, process_type, db_type, source_db_name, target_db_name, source_host_name,
                                    target_host_name)
        result['found_process'] = found_process

        if len(found_process) == 0 :
            result['status'] = 'failed'
            err_message = "Error! No Process found for operation."
            module.exit_json(changed=True, meta=result,msg=err_message)

        for process in found_process:
            queue_name = process['Queue Name']
            run_cmd = "%s %s queue %s" % (state, process_type, queue_name)
            cmd = """
            source %s/.profile_u%s; cd %s;
            ./sp_ctrl << EOF
            %s
            EOF
            """ % (shareplex_bin_dir, port, shareplex_bin_dir, run_cmd)
            module.run_command(cmd, use_unsafe_shell=True)
        # 5. post verify
        time.sleep(5)
        cmd = """
                           source %s/.profile_u%s; cd %s;
                           ./sp_ctrl << EOF
                           lstatus
                           EOF
                           """ % (shareplex_bin_dir, port, shareplex_bin_dir)
        rc, post_lstatus_res, err = module.run_command(cmd, use_unsafe_shell=True)
        post_lstatus = parser_lstatus(post_lstatus_res)
        verify_process = hit_process(post_lstatus, process_type, db_type, source_db_name, target_db_name,
                                   source_host_name, target_host_name)
        result['verify_process'] = verify_process
        post_verify_flag = True
        post_verify_str = ""
        if state == "start":
            post_verify_str = "Running"
        if state == "stop":
            post_verify_str = "Stopped"
        for p in verify_process:
            if post_verify_str not in p['State']:
                post_verify_flag = False
                result['err_message'] = "post verify fail"
                break
        if len(found_process) != len(verify_process):
            post_verify_flag = False
            result['err_message']  = "post verify fail"
        result['post_verify'] = post_verify_flag
    module.exit_json(changed=True, meta=result)

if __name__ == "__main__":
    main()