
from ansible.module_utils.basic import AnsibleModule

# parse list config cmd content
def parse_list_config(config_file_content):
    config_files = {}
    for line in config_file_content.splitlines():
        if str(line).strip() and (str(line).strip().find("Inactive") > 0 or str(line).strip().find("Active") > 0):
            filename = str(line).strip().split()[0]
            status = str(line).strip().split()[1]
            datasource = str(line).strip().split()[2]
            item = {}
            item['file'] = filename
            item['status'] = status
            item['datasource'] = datasource
            config_files[filename] = item
    return config_files

def main():
    module_args = dict(
        port=dict(type='str', required=True),
        config_file=dict(type='str', required=True),
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
    config_file_path = module.params['config_file']
    config_file = config_file_path.split("/")[-1]
    # result['config_file'] = config_file

    # 1. check parameter
    if state not in ["activate", "deactivate"]:
        result['status'] = 'failed'
        err_message = "Error! parameter status must in [ activate, deactivate ]"
        module.exit_json(changed=True, meta=result, msg=err_message)
    cmd = " ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'| cut -d '/' -f  1-3 " % (port)
    rc, out, err = module.run_command(cmd, use_unsafe_shell=True)
    # 2. check port status
    if len(str(out)) == 0:
        result['status'] = 'failed'
        err_message = "Error! Shareplex port %s is not running." % (port)
        module.exit_json(changed=True, meta=result, msg=err_message)
    # 3. check config validity
    shareplex_bin_dir = "%s/bin" % (str(out).rstrip())
    cmd = """
    source %s/.profile_u%s; cd %s;
    ./sp_ctrl << EOF
    list config
    show
    EOF
    """ % (shareplex_bin_dir, port, shareplex_bin_dir)
    rc, out3, err = module.run_command(cmd, use_unsafe_shell=True)
    config_files = parse_list_config(out3)
    # result['config_files'] = config_files
    if not config_file in config_files.keys():
        result['status'] = 'failed'
        err_message = "Error! This config file not found."
        module.exit_json(changed=True, meta=result, msg=err_message)
    # 4. activate/deactivate config file
    cmd = """
        source %s/.profile_u%s; cd %s;
        ./sp_ctrl << EOF
        %s config %s
        show
        EOF
        """ % (shareplex_bin_dir, port, shareplex_bin_dir,state,config_file)
    module.run_command(cmd, use_unsafe_shell=True)
    # 5. post verify
    cmd = """
        source %s/.profile_u%s; cd %s;
        ./sp_ctrl << EOF
        list config
        show
        EOF
        """ % (shareplex_bin_dir, port, shareplex_bin_dir)
    rc, out4, err = module.run_command(cmd, use_unsafe_shell=True)
    post_config_files = parse_list_config(out4)
    post_file = post_config_files[config_file]
    result['post_verify'] = post_file


    module.exit_json(changed=True, meta=result)


if __name__ == "__main__":
    main()