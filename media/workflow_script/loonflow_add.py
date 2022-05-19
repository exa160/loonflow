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
# address = 'localhost:5008'
# username = 'loonflow'
app_name = 'loonflow'
username = 'admin'
token = '16a39cac-7501-11ec-8471-a0a8cd5bbc9b'

full_path = r'./media/ticket_file/派单平台导出全量清单.csv'
in_path = r'./media/ticket_file/导入.xlsx'

logger = logging.getLogger('django')

# # get
# get_data = dict(per_page=20, category='all')
# r = requests.get('http://127.0.0.1:8000/api/v1.0/tickets', headers=headers, params=get_data)
# result = r.json()


class MyThread(threading.Thread):  
    def __init__(self, func, args=()):  
        super(MyThread, self).__init__()  
        self.func = func  
        self.args = args  
  
    def run(self):  
        self.result = self.func(*self.args)  # 在执行函数的同时，把结果赋值给result,  
        # 然后通过get_result函数获取返回的结果  
  
    def get_result(self):  
        try:  
            return self.result  
        except Exception as e:  
            return None, None   

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

def send_loonflow(data_dict, trans_id):
    # logger.info(data_dict)
    data = {"title": '摄像头离线：{}-{}-{}'.format(data_dict.get('COUNTY_NAME'), data_dict.get('CAMERA_NAME'), data_dict.get('WY_ID'))[:49],
            "county_name": data_dict.get('COUNTY_NAME'),
            "platform": data_dict.get('PT'),
            "industry_type": data_dict.get('HY_TYPE'),
            "belong_customer": data_dict.get('CLIENT_NAME')[:50],
            "camera_ip": data_dict.get('IP_ADDRESS'),
            "camera_id": data_dict.get('WY_ID'),
            "camera_name": data_dict.get('CAMERA_NAME')[:50],
            "camera_add": data_dict.get('CAM_ADD'),
            "group_bill": data_dict.get('GROUP_BILL'),
            "delivered": data_dict.get('JW'),
            "fault_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "is_restore": data_dict.get('STATUS'),
            "file_data": '',
            "transition_id": trans_id,
            "workflow_id": 1,
            "username": action_from
            }
    # data = dict(per_page=20, category='all')
    # request_new(data)
    #t = MyThread(ticket_base_service_ins.new_ticket, args=(data, 'loonflow'))
    t = MyThread(request_new, args=(data,))
    t.start()
    return data_dict.get('WY_ID'), t


def get_workflow_status():
    per_page = 20000
    data = dict(per_page=per_page, category='all', act_state_id=1, workflow_ids=1)
    r = requests.get(f'http://{address}/api/v1.0/tickets', headers=get_header(), params=data)
    
    total = r.json().get('data').get('total')
    if total > per_page:
        data = dict(per_page=total, category='all', act_state_id=1, workflow_ids=1)
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
    if len(result.get('data', {}).get('value', [])) == 0:
        time.sleep(5)
        result = get_workflow_status()
    camera_id_list = {}
    for data_dict in result.get('data', {}).get('value', []):
        # print(data_dict['id'],data_dict['title'])
        if data_dict['state_id'] == 2:
            continue
        id = data_dict['id']
        camera_id = ''
        if False:
            title = data_dict['title'].split('-')
            if len(title) > 1:
                camera_id = title[-1]
        else:
            tk_val = get_ticket_data(data_dict['id'])
            for data_dict in tk_val['data']['value']['field_list']:
                if data_dict['field_key'] == 'camera_id':
                    camera_id = data_dict['field_value']
                    camera_id_list.update({id:camera_id})
                    break
    
    # logger.info(f'inline_dict:{camera_id_list}')
    for t_id, c_id in camera_id_list.items():
        if c_id in id_list:
            res = close_ticket(t_id)
            if res.json()['code'] == 0:
                res_id_list.append(camera_id)
                logger.info(f'{camera_id}, 已恢复')

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
    # logger.info(result)
    return result


def load_file(full_path, _in_path):
    """
    读取文件
    :param full_path:
    :param in_path:
    :return:
    """
    all_data = pd.read_csv(full_path, index_col=0, encoding='ansi')
    if all_data.index.name == 'PT':
        all_data = all_data.reset_index(drop=False)
    else:
        all_data = all_data.reset_index(drop=True)
    in_data = pd.read_excel(_in_path)
    return all_data, in_data


def get_outline_camera(all_data, in_data):
    outline_data = in_data[in_data['STATUS'].str.contains('离线')]
    add_data = all_data[(all_data['WY_ID'].isin(outline_data['WY_ID'])) & (all_data['STATUS'].str.contains('在线'))]
    return all_data, add_data


def get_inline_camera(all_data, in_data):
    outline_data = in_data[in_data['STATUS'].str.contains('在线')]
    # add_data = all_data[(all_data['WY_ID'].isin(outline_data['WY_ID'])) & (all_data['STATUS'].str.contains('离线'))]
    add_data = outline_data
    id_list = add_data['WY_ID'].to_list()
    # logger.info(f'list:{str(id_list)}')
    return all_data, add_data, id_list


def send_msg(add_list, fail_list):
    msg_text = f'发现{len(add_list)+len(fail_list)}摄像头离线，共派出{len(add_list)}单'
    if len(fail_list) > 0:
        warn_msg = f'{len(fail_list)}单派发失败，详情见日志'
        msg_text += f'，{warn_msg}'
        # logger.error(msg_text)
    # logger.info(msg_text)
    return msg_text
    # data = dict(workflow_id= 1, username= "admin", appname='loonflow', title='工单派发结束提醒', text=msg_text)
    # r = requests.post(f'http://{address}/api/v1.0/workflow/mailmsg', headers=get_header(), json=data)

def send_dataflow(add_data):
    add_list = []
    fail_list = []
    t_pool = []
    if len(add_data) > 0:
        trans_dict = get_trans()
        logger.info('threading active->{}'.format(threading.activeCount()))
        for index, i in add_data.fillna('未知').T.items():
            i_dict = i.to_dict()
            trans_id = trans_dict.get(i_dict.get('COUNTY_NAME').strip('区市县'), -1)
            if trans_id == -1:
                fail_list.append(i_dict.get('WY_ID'))
                logger.info('ID:{} , 失败'.format(i.to_dict().get('WY_ID')))
                continue
            t = send_loonflow(i_dict, trans_id)
            t_pool.append(t)
            while threading.activeCount() > 8:
                pass
                #logger.info('threading active over 10->{}'.format(threading.activeCount()))
                time.sleep(1)
        for wy_id, t in t_pool:
            t.join()
            res, msg = t.get_result()
            if res:
                logger.info('ID:{} , 已追加'.format(wy_id))
                add_list.append(wy_id)
    send_msg(add_list, fail_list)
        
    return add_list, fail_list


def set_outline(full_data, wy_id_list):
    """
    更新为离线
    :param full_data:
    :param wy_id_list:
    :return:
    """
    o = full_data[full_data['WY_ID'].isin(wy_id_list)].copy()
    if len(o) > 0:
        o['STATUS'] = '离线'
        full_data.update(o)
    return full_data, o


def set_inline(full_data, wy_id_list):
    """
    更新为在线
    :param full_data:
    :param wy_id_list:
    :return:
    """
    o = full_data[full_data['WY_ID'].isin(wy_id_list)].copy()
    if len(o) > 0:
        o['STATUS'] = '在线'
        o['OUT_RESAON'] = '自动恢复（查看已经在线）'
        full_data.update(o)
        logger.info(f'发现在线：{len(o)},已自动关单')
    return full_data, o

        
def run():
    flag = r'./media/ticket_file/flag'
    if not os.path.exists(flag):
        with open(flag,'w') as a:
            pass

    file_path, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'file_update')  # ticket_id会通过exec传过来
    file_path_values = msg.get('value')
    if '全量' in file_path_values:
        print('全量表上传')
        return True, '全量表上传'
    os.chmod(flag, 33060)  # 锁标记
    try:
        all_data, in_data = load_file(full_path, f'.{file_path_values}')
        # 在线 -> 离线
        full_data, add_data = get_outline_camera(all_data, in_data)
        wy_id_list, fail_id_list = send_dataflow(add_data)
        full_data, o = set_outline(full_data, wy_id_list)
    finally:
        os.chmod(flag, 33206)  # 解锁标记

    # 离线 -> 在线
    full_data2, add_data, id_list = get_inline_camera(full_data, in_data)
    res_id_list = check_all_id(id_list)
    # full_data, o = set_inline(full_data, res_id_list)

    full_data.to_csv(full_path, quoting=1, encoding='ansi')
    # msg_text = f'完成派单,离线：{len(wy_id_list)+len(fail_id_list)}，派出：{len(wy_id_list)}'
    msg_text = f'离线:{len(wy_id_list)+len(fail_id_list)}，派出:{len(wy_id_list)}'
    if len(res_id_list) > 0:
        msg_text += f'；关单:{len(res_id_list)}'
    if len(fail_id_list) > 0:
        msg_text += f'；失败:{len(fail_id_list)}'
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
