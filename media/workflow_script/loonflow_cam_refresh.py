import os
import time
import traceback
import logging

logger = logging.getLogger('django')
flag = r'./media/ticket_file/flag'
if not os.path.exists(flag):
    with open(flag, 'w') as a:
        pass
start_time = time.time()
while os.stat(flag).st_mode != 33206:
    time.sleep(5)
    if (time.time() - start_time) > 300:
        print('锁定300s超时')
        break

os.chmod(flag, 33060)
from apps.ticket.models import TicketRecord
from service.ticket.ticket_base_service import ticket_base_service_ins

import pandas as pd

"""
1.因为使用execfile/exec执行脚本，脚本中会跟随celery的执行环境
2.ticket_id和action_from参数会通过调用的时候传递过来，可以直接使用.可以使用ticket_id获取ticket相关的信息
3.因为使用execfile/exec执行脚本, 不得使用if __name__ == '__main__'
4.本脚本场景为服务器权限申请，工单中有自定义字段:host_ip
"""
full_path = r'./media/ticket_file/派单平台导出全量清单.csv'


def load_file(f, f_path):
    data_dest = pd.read_csv(f, index_col=0, encoding='ansi')
    if data_dest.index.name == 'PT':
        data_dest = data_dest.reset_index(drop=False)
    else:
        data_dest = data_dest.reset_index(drop=True)
    df_add = pd.read_excel(f_path)
    return data_dest, df_add


def run():
    file_update, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'file_update')  # ticket_id会通过exec传过来
    res_print = ''
    if file_update:
        file_update_value = msg.get('value')
        data_dest, df_add = load_file(full_path, f'.{file_update_value}')
        df_add = df_add.fillna('')
        df_add['STATUS'] = '在线'
        df_add_new = df_add.set_index('WY_ID')
        data_dest_new = data_dest.set_index('WY_ID')

        drop_source = df_add_new[df_add_new['点位增删'] == '删除'][data_dest_new.columns.to_list()]
        add_source = df_add_new[df_add_new['点位增删'] == '增加'][data_dest_new.columns.to_list()]

        drop_data = drop_source[drop_source.index.isin(data_dest_new.index)]
        add_data = add_source[~add_source.index.isin(data_dest_new.index)]
        update_data = add_source[add_source.index.isin(data_dest_new.index)]

        print(f'新增点位：{len(add_data)}，删除点位：{len(drop_data)}，更新点位：{len(update_data)}')
        # drop data
        data_dest_new1 = data_dest_new.drop(drop_data.index.to_list())
        # append data
        data_dest_new2 = data_dest_new1.append(add_data)
        # update data # 在中间修改会导致数据被覆盖
        data_dest_new2.update(update_data)
        data_dest_new2.reset_index().to_csv(full_path, quoting=1, encoding='ansi')
    else:
        res_print = '无上传数据'
    print(res_print)
    return True, res_print


try:
    run()
except Exception as e:
    logger.error(traceback.format_exc())
    print(e)
os.chmod(flag, 33206)
