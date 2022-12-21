import time

from ansible.module_utils.basic import AnsibleModule

def main():
    module_args = dict(
        port=dict(type='str', required=True),
        state=dict(type='str', required=True)
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
    result['pre_cmd'] = "state=%s, port=%s" %(state,port)
    # 1. check parameter
    if state not in ["start","stop"]:
        result['status'] = 'failed'
        result['err_message'] = "Error! parameter status must in [ start, stop ]"
        module.exit_json(changed=True, meta=result)

    # 2. check shareplex port initial status
    cmd = " ps -ef | grep sp_cop | grep -v grep | grep %s | wc -l " % (port)
    rc, out1, err = module.run_command(cmd, use_unsafe_shell=True)
    port_count = int(str(out1).strip())

    if state == "start" and port_count != 0:
        result['status'] = 'failed'
        result['err_message'] = "Error! Port %s is running, it cannot be started again." %(port)
        module.exit_json(changed=True, meta=result)
    if state == "stop" and port_count == 0:
        result['status'] = 'failed'
        result['err_message'] = "Error! Port %s is stopped, it cannot be stopped again." %(port)
        module.exit_json(changed=True, meta=result)

    # 3. find shareplex bin dir for specific port
    cmd = "cat /etc/oraport | grep %s | cut -d ':' -f 2" %(port)
    c, out2, err = module.run_command(cmd, use_unsafe_shell=True)
    shareplex_bin_dir = str(out2).strip()

    # 4. exec start/stop port
    # 4.1 start
    if state == "start":
        cmd = """
        source %s/.profile_u%s; cd %s;
        ./sp_cop -u%s & << EOF
        show
        EOF
        """ % (shareplex_bin_dir, port, shareplex_bin_dir, port)
    # 4.2 stop
    else:
        cmd = """
        source %s/.profile_u%s; cd %s;
        ./sp_ctrl << EOF
        shutdown
        show
        EOF
        """ % (shareplex_bin_dir, port, shareplex_bin_dir)
    module.run_command(cmd, use_unsafe_shell=True)

    # 5. post verify
    time.sleep(3)
    cmd = "ps -ef | grep sp_cop | grep -v grep | grep %s  | wc -l " % (port)
    rc, count, err = module.run_command(cmd, use_unsafe_shell=True)
    post_verify_count = str(count).strip()
    if (state == "start" and post_verify_count == "1") or (state == "stop" and post_verify_count == "0"):
        result['post_verify'] = True
    else:
        result['post_verify'] = False
        result['status'] = 'failed'
        result['err_message'] = "Error! post verify failed."

    module.exit_json(changed=True, meta=result)

if __name__ == "__main__":
    main()