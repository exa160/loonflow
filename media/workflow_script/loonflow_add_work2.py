import pandas as pd
import logging
from apps.ticket.models import TicketRecord
from service.ticket.ticket_base_service import ticket_base_service_ins

import hashlib
import threading
import requests
import time
import os

address = 'localhost:10000'
address = 'localhost:5008'
# username = 'loonflow'
app_name = 'JHGA'
username = 'admin'
token = '9e8c0062-aa4f-11ec-91a4-a0a8cd5bbc9b'

logger = logging.getLogger('django')


# # get
# get_data = dict(per_page=20, category='all')
# r = requests.get('http://127.0.0.1:8000/api/v1.0/tickets', headers=headers, params=get_data)
# result = r.json()


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.result = [None, None]
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)  # 在执行函数的同时，把结果赋值给result,  
        # 然后通过get_result函数获取返回的结果  

    def get_result(self):
        return self.result


def get_header():
    timestamp = str(time.time())[:10]
    ori_str = timestamp + token
    signature = hashlib.md5(ori_str.encode(encoding='utf-8')).hexdigest()
    headers = dict(signature=signature, timestamp=timestamp, appname=app_name, username=action_from)
    return headers


def get_trans():
    r = requests.get(f'http://{address}/api/v1.0/workflows/1/init_state', headers=get_header())
    result = r.json()
    res_dict = {}
    for i in result['data']['transition']:
        res_dict.update({i['transition_name'].split('-')[-1]: i['transition_id']})
    return res_dict


def request_new(data):
    r = requests.post(f'http://{address}/api/v1.0/tickets', headers=get_header(), json=data)
    result = r.json()
    code = result.get('code', -1)
    if code == 0:
        return True, result
    return False, result


def send_loonflow(data_dict):
    # logger.info(data_dict)
    data = {"title": '公安摄像头离线：{}-{}-{}'.format(data_dict.get('县市'), data_dict.get('摄像机名称'), data_dict.get('国标编码'))[:49],
            "platform": data_dict.get('平台'),
            "industry_type": data_dict.get('行业分类'),
            "county_name": data_dict.get('县市'),
            "belong_customer": data_dict.get('所属客户')[:50],
            "camera_id": data_dict.get('国标编码'),
            "camera_name": data_dict.get('摄像机名称')[:50],
            "deduction": data_dict.get('扣分项'),
            # "project": data_dict.get('所属项目'),
            # "project_status": data_dict.get('项目状态'),
            # "delivered": data_dict.get('是否交维'),
            # "maintenance_unit": data_dict.get('维护单位'),
            # "project_principal": data_dict.get('一线维护人员'),
            "fault_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "transition_id": 69,
            "file_data": '',
            "workflow_id": 4,
            "username": action_from
            }
    # logger.info(data)
    # data = dict(per_page=20, category='all')
    # request_new(data)
    # t = MyThread(ticket_base_service_ins.new_ticket, args=(data, 'loonflow'))
    t = MyThread(request_new, args=(data,))
    t.start()
    return data_dict.get('国标编码'), t


def get_workflow_status():
    per_page = 20000
    data = dict(per_page=per_page, category='all', act_state_id=1, workflow_ids=4)
    r = requests.get(f'http://{address}/api/v1.0/tickets', headers=get_header(), params=data)

    total = r.json().get('data').get('total')
    if total > per_page:
        data = dict(per_page=total, category='all', act_state_id=1, workflow_ids=4)
        r = requests.get(f'http://{address}/api/v1.0/tickets', headers=get_header(), params=data)
    result = r.json()
    return result


def get_ticket_data(id):
    r = requests.get(f'http://{address}/api/v1.0/tickets/{id}', headers=get_header())
    result = r.json()
    return result


def check_all_id(id_list):
    id_list = set(id_list)
    res_id_list = []
    result = get_workflow_status()
    camera_id_list = {}
    # logger.info(result)
    for data_dict in result['data']['value']:
        # print(data_dict['id'],data_dict['title'])
        id = data_dict['id']
        tk_val = get_ticket_data(data_dict['id'])
        for data_dict2 in tk_val['data']['value']['field_list']:
            if data_dict2['field_key'] == 'camera_id':
                camera_id = data_dict2['field_value']
                camera_id_list.update({id: camera_id})
                break
    # logger.info(f'inline_dict:{camera_id_list}')
    for t_id, c_id in camera_id_list.items():
        if c_id in id_list:
            res_id_list.append(c_id)
            logger.info(f'{c_id}, 已存在')

    return res_id_list


def close_ticket(id):
    data = dict(is_restore='已恢复,自动恢复（查看已经在线）')
    r = requests.patch(f'http://{address}/api/v1.0/tickets/{id}/fields', headers=get_header(), json=data)
    # print(r, r.json())
    data = dict(suggestion='导入状态为已恢复', state_id=2)
    # r = requests.post(f'http://{address}/api/v1.0/tickets/{id}/close', headers=get_header(), json=data)
    r = requests.put(f'http://{address}/api/v1.0/tickets/{id}/state', headers=get_header(), json=data)
    if r.status_code != 10:
        return r
    result = r.json()
    logger.info(result)
    return result


def load_file(_in_path):
    """
    读取文件
    :param _in_path:
    :return:
    """
    in_data = pd.read_excel(_in_path)
    return in_data


def send_msg(add_list, fail_list):
    msg_text = f'共派出{len(add_list)}单'
    if len(fail_list) > 0:
        warn_msg = f'{len(fail_list)}单派发失败，详情见日志'
        msg_text += f'，{warn_msg}'
        logger.error(msg_text)
    logger.info(msg_text)
    return msg_text
    # data = dict(workflow_id= 1, username= "admin", appname='loonflow', title='工单派发结束提醒', text=msg_text)
    # r = requests.post(f'http://{address}/api/v1.0/workflow/mailmsg', headers=get_header(), json=data)


def send_dataflow(add_data):
    add_list = []
    fail_list = []
    t_pool = []
    if len(add_data) > 0:
        logger.info('threading active->{}'.format(threading.activeCount()))
        for index, i in add_data.fillna('未知').T.items():
            i_dict = i.to_dict()

            # trans_id = trans_dict.get(i_dict.get('COUNTY_NAME').strip('区').strip('市').strip('县'), -1)
            t = send_loonflow(i_dict)
            t_pool.append(t)
            while threading.activeCount() > 8:
                pass
                # logger.info('threading active over 10->{}'.format(threading.activeCount()))
                time.sleep(1)
        for wy_id, t in t_pool:
            t.join()
            res, msg = t.get_result()
            if res:
                logger.info('ID:{} , 已追加'.format(wy_id))
                add_list.append(wy_id)
            else:
                logger.info(msg)
    send_msg(add_list, fail_list)

    return add_list, fail_list


def run():
    file_path, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'file_update')  # ticket_id会通过exec传过来
    file_path_values = msg.get('value', '')
    if file_path_values == '':
        return True, '文件上传失败'
    in_data = load_file(f'.{file_path_values}')
    # 派单
    id_list = in_data['国标编码'].to_list()
    res_id_list = check_all_id(id_list)
    send_id = set(id_list) - set(res_id_list)
    send_data = in_data[in_data['国标编码'].isin(send_id)]
    wy_id_list, fail_id_list = send_dataflow(send_data)

    msg_text = f'完成派单，共{len(id_list)}离线，派出{len(wy_id_list)}单'
    if len(res_id_list) > 0:
        msg_text += f'；已存在：{len(res_id_list)}单'
    if len(fail_id_list) > 0:
        msg_text += f'；{len(fail_id_list)}单派发失败'
    print(msg_text)
    return True, msg_text


run()

# if __name__ == '__main__':
#     all_data, in_data = load_file(full_path, in_path)
#     # 在线 -> 离线
#     full_data, add_data = get_outline_camera(all_data, in_data)
#     wy_id_list = send_dataflow(add_data)
#     full_data, o = set_outline(full_data, wy_id_list)

#     # 离线 -> 在线
#     full_data, add_data, id_list = get_inline_camera(full_data, in_data)
#     res_id_list = check_all_id(id_list)
#     full_data, o = set_inline(full_data, res_id_list)

#     full_data.to_excel(full_path, quoting=1, encoding='ansi')
