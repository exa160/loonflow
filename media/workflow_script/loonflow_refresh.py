import os
import time
import traceback
import logging
logger = logging.getLogger('django')

flag = r'./media/ticket_temp/flag_flow01'
if not os.path.exists(flag):
    with open(flag,'w') as a:
        pass
start_time = time.time()
while os.stat(flag).st_mode != 33206:
    time.sleep(5)
    if (time.time() - start_time) > 3600:
        logger.warning('锁定3600s超时')
        break
        
os.chmod(flag, 33060)
from apps.ticket.models import TicketRecord
from service.ticket.ticket_base_service import ticket_base_service_ins
import json
import pandas as pd

"""
1.因为使用execfile/exec执行脚本，脚本中会跟随celery的执行环境
2.ticket_id和action_from参数会通过调用的时候传递过来，可以直接使用.可以使用ticket_id获取ticket相关的信息
3.因为使用execfile/exec执行脚本, 不得使用if __name__ == '__main__'
4.本脚本场景为服务器权限申请，工单中有自定义字段:host_ip
"""
full_path = r'./media/ticket_temp/社会面全量点位信息.csv'


def load_file():
    return pd.read_csv(full_path, index_col=0, encoding='ansi').reset_index(drop=True)


def demo_script_call():
    # 获取工单信息ip地址信息
    ticket_base_service_ins.new_ticket(data_dict, 'loonflow')
    # username, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'creator')  # ticket_id会通过exec传过来
    file_path, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'file_update')  # ticket_id会通过exec传过来
    # 你也可以获取工单的其他字段信息，包括自定义字段的值。根据获取的值做后续处理
    with open('D:\Python\loonflow_add\j.json', 'w') as c:
        c.write(str(locals()))
    return True, ''


def set_inline(full_data, wy_id):
    """
    更新为离线
    :param full_data:
    :param wy_id_list:
    :return:
    """
    o = full_data[full_data['WY_ID'] == wy_id].copy()
    restore_reason, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'is_restore')
    restore_reason, msg2 = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'is_restore_ct')
    if len(o) > 0:
        o['STATUS'] = '在线'
        m1 = msg.get('value', '')
        m2 = msg2.get('value', '')
        if m1 is None:
            m1 = ''
        if m2 is None:
            m2 = ''
        o['OUT_RESAON'] = f'{m1};{m2}'
        full_data.update(o)
        return full_data, True
    return full_data, False


def run():
    is_restore, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'is_restore')  # ticket_id会通过exec传过来
    res_print = ''
    if is_restore:
        is_restore_value = msg.get('value')
        if is_restore_value.split(',')[0] == '已恢复':
            camera_id, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'camera_id')  # ticket_id会通过exec传过来
            if camera_id:
                camera_id_values = msg.get('value')
                all_data = load_file()
                all_data, status = set_inline(all_data, camera_id_values)
                if status:
                    all_data.to_csv(full_path, quoting=1, encoding= 'ansi')
                    res_print = '状态已刷新'
                else:
                    res_print = f'未找到ID：{camera_id_values} 记录'
            else:
                res_print = '工单无ID记录'
        else:
            res_print = '工单为仍离线'
    else:
        res_print = '无状态值'
    print(res_print)
    return True, res_print

try:
    run()
except Exception as e:
    logger.error(traceback.print_exception())
    logger.error(e)
os.chmod(flag, 33206)