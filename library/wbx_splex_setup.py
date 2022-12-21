import pathlib
from datetime import datetime

from ansible.module_utils.basic import AnsibleModule

def Log(msg):
    message = "%s :: %s\n" %(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),msg)
    return message

def main():
    module_args = dict(
        port=dict(type='str', required=True)
    )
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=False
    )
    result = dict(
        status='successed',
        stdout="",
        stderr=""
    )

    port = module.params['port']
    # 1. verify port is already running
    cmd = ''' ps -ef | grep sp_ | grep -e "-u *%s" | wc -l ''' %(port)
    rc, count, err = module.run_command(cmd, use_unsafe_shell=True)
    count_running = str(count).strip()
    result['count_running'] = count_running
    if int(count_running) > 1:
        result['status'] = 'failed'
        result['stderr'] = "Error! Port %s is alredy running, so cannot continue with the setup." %(port)
        module.exit_json(changed=True, meta=result)
    else:
        Log_msg = ""
        _SCRIPT_DIR = "/pgsplex/port_setup"
        _SCRIPT_DIR_Logs = "/pgsplex/port_setup/logs"
        _license_value = "DZHCEZ8VJ8V54WPGAJ2NL73N8SQVZR6Z7B"
        _license_customer = "CISCO SYSTEMS INC"
        _LOG_FILE = "%s/logs/splex_port_setup_%s_%s.log" % (_SCRIPT_DIR, port, datetime.now().strftime("%Y%m%d%H%M%S"))
        try:
            Log_msg += Log("_SCRIPT_DIR: %s" % (_SCRIPT_DIR))
            pathlib.Path(_SCRIPT_DIR_Logs).mkdir(parents=True,exist_ok=True)
            result['_LOG_FILE'] = _LOG_FILE
            pathlib.Path(_LOG_FILE).touch(mode=0o666, exist_ok=True)
            Log_msg += Log("_LOG_FILE: %s" % (_LOG_FILE))
        except Exception as e:
            Log_msg += Log("Error: %s" % (str(e)))
            cmd = " echo -e \"{0}\">>{1}".format(Log_msg, _LOG_FILE)
            module.run_command(cmd, use_unsafe_shell=True)

        cmd = " echo -e \"{0}\">>{1}".format(Log_msg, _LOG_FILE)
        module.run_command(cmd, use_unsafe_shell=True)

    module.exit_json(changed=True, meta=result)

if __name__ == "__main__":
    main()