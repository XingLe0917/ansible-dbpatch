import re

import prettytable as pt
from prettytable import from_html

def hit_process(lstatus,process_type,data_src,data_dst):
    if "post" == process_type:
        for process in lstatus:
            data_host = "%s-%s" % (data_src, data_dst)
            if process_type.lower() == process['Process'].lower() and process['Data/Host'] and process[
                'Data/Host'] == data_host:
                return process
    else:
        for process in lstatus:
            if process_type.lower() == process['Process'].lower():
                return process
    return {}

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
                    process['process'] = pro_arr[0]
                    process['status'] = pro_arr[1]
                else:
                    key = str(line.split(":")[0]).strip()
                    value = str(line.split(":")[1]).strip()
                    process[key] = value
        process_list.append(process)
    return process_list




if __name__ == "__main__":
    # lstatus = '''
    # Detailed Status for amdbpgtest
    # Process          State                             PID     Running   Since
    # ---------------  ------------------------------  --------  --------------------
    # Cop              Running                          4137945  02-Nov-22 08:57:42
    # Capture          Running                          4138394  02-Nov-22 08:57:42
    #   Data/Host:   r.pgguweb
    # Import           Running                          4174842  02-Nov-22 08:58:18
    #   Data/Host:   amdbpgtest02
    #   Queue Name:  amdbpgtest02
    # Post             Stopped by user
    #   Data/Host:   r.pggvweb-r.pgguweb
    #   Queue Name:  amdbpgtest02
    # Read             Running                          4138405  02-Nov-22 08:57:42
    #   Data/Host:   r.pgguweb
    # Export           Running                          4138400  02-Nov-22 08:57:42
    #   Data/Host:   amdbpgtest02.webex.com
    #   Queue Name:  amdbpgtest
    # Cmd & Ctrl       Running                          3007531  04-Nov-22 01:00:31
    #   Data/Host:   amdbpgtest.webex.com
    #
    #
    # Queues:
    #  Type    # Msgs   Size (Mb)  Age (mn) Oldest Msg Time    Newest Msg Time
    # ------- --------- ---------- -------- ------------------ ------------------
    # Post            0          1        0 22-Oct-22 03:45:22 22-Oct-22 03:45:22
    #   Queue Name:       amdbpgtest02
    #   DataSrc-DataDst:  r.pggvweb-r.pgguweb
    # Capture      1096          6        0 04-Nov-22 01:00:38 04-Nov-22 01:00:46
    #   Queue Name:       r.pgguweb
    # Export       8703          8        5 04-Nov-22 00:55:02 04-Nov-22 01:00:46
    #   Queue Name:       amdbpgtest
    #
    # System is used as a source machine
    # Replication Configurations:
    #
    #   Replication active from "auto.20221102_085552.cfg", actid 71, for database r.pgguweb since 02-Nov-22 08:55:53
    #   '''
    # res = parser_lstatus(lstatus)
    # print(res)

    lstatus = [
                {
                    "Process": "Cop",
                    "State": "Running"
                },
                {
                    "Data/Host": "r.pgguweb-r.pggvweb",
                    "Process": "Post",
                    "Queue Name": "amdbpgtest",
                    "State": " Running"
                },
                {
                    "Data/Host": "amdbpgtest",
                    "Process": "Import",
                    "Queue Name": "amdbpgtest",
                    "State": " Running"
                },
                {
                    "Data/Host": "amdbpgtest02.webex.com",
                    "Process": "Cmd & Ctrl",
                    "State": " Running"
                },
                {
                    "Data/Host": "amdbpgtest02.webex.com",
                    "Process": "Cmd & Ctrl",
                    "State": " Running"
                }
            ]
    process_type = "post"
    data_src = "r.pgguweb"
    data_dst = "r.pggvweb"
    process = hit_process(lstatus,process_type,data_src,data_dst)
    print(process)