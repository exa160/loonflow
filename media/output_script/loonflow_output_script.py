from collections import defaultdict
from datetime import datetime
import logging
import traceback

import pandas as pd
from apps.ticket.models import TicketOutFile

from service.ticket.ticket_base_service import ticket_base_service_ins
from service.workflow.workflow_custom_field_service import workflow_custom_field_service_ins

logger = logging.getLogger('django')


def time_trans(str_data):
    try:
        res = datetime.strptime(str(str_data),'%Y-%m-%d %H:%M:%S')
    except Exception as e:
        res = datetime.strptime('2099-12-12 12:12:12','%Y-%m-%d %H:%M:%S')
    return res
    

def time_trans2(d_data):
    seconds = d_data.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f'{int(hours)}:{int(minutes)}:{int(seconds)}'

def is_timeout_hours(d_data, limit=4):
    seconds = d_data.total_seconds()
    hours = seconds // 3600
    if hours >= limit:
        return True
    else:
        return False


def output_data(ticket_ids, username, file_path, file_obj_id):
    out_dict = defaultdict(list)
    try:
        key_dict = {}
        for ticket_id in ticket_ids:
            if ticket_id == -1:
                continue
            flag, ticket_result = ticket_base_service_ins.get_ticket_detail(ticket_id, username)

            if flag:
                
                state_1 = ''
                output_suggestion = ''
                output_log = ''
                state_1_time = ''
                
                t_key_list = [t_dict['field_name'] for t_dict in ticket_result['field_list']]  # 工单对应所有字段
                t_value_list = [t_dict['field_value'] for t_dict in ticket_result['field_list']]  # 工单对应所有字段值
                t_flowid = ticket_result['workflow_id']
                state_info = ticket_result['state_info']
                
                state_latest = state_info.get('name', '')
                gmt_created_time = ticket_result.get('gmt_created', '')
                state_latest_time = ticket_result.get('gmt_modified', '')
                
                base_value_list = [state_latest, state_latest_time]
                second_value_list = []
                base_key_list = ['最新状态', '最后操作时间']
                second_key_list = []
                
                # if t_flowid != 1:
                #     continue
                # 获取工作流对应所有字段
                k_list = key_dict.get(t_flowid, [])
                if len(k_list) == 0:
                    flag, custom_field_dict = workflow_custom_field_service_ins.get_workflow_custom_field(
                        t_flowid)
                    t_list = []
                    for value in custom_field_dict.values():
                        field_name = value['field_name']
                        t_list.append(field_name)
                    key_dict.update({t_flowid: t_list + base_key_list})
                flag, log_result = ticket_base_service_ins.get_ticket_flow_log(ticket_id, username, 20, 1, 0, 0)
                log_pages = log_result.get('paginator_info', {}).get('total', 0)
                # 获取操作日志
                log_line = []
                log_res = log_result.get('ticket_flow_log_restful_list')
                for i in log_res:
                    log_line.append(f"{i['gmt_created']}：用户'{i['participant_info']['participant_alias']}' 在状态'{i['state']['state_name']}'下执行了'{i['transition']['transition_name']}'，处理意见：{i['suggestion']}".replace('\r\n',''))
                output_log = '\r\n'.join(log_line)

                
                if t_flowid == 1:
                    if len(log_res) > 1:
                        state_1 = log_res[1].get('state', {}).get('state_name', '')
                        state_1_time = log_res[1].get('gmt_created', '')
                        output_suggestion = log_res[-2].get('suggestion', '')
                        diff_date = time_trans2(time_trans(state_1_time) - time_trans(gmt_created_time))
                        
                        second_value_list = [state_1, state_1_time, diff_date, output_suggestion, output_log]
                        second_key_list = ['IT处理', 'IT处理时间', 'IT一次处理耗时', '操作意见', '操作日志']
                elif t_flowid == 4:
                    if len(log_res) > 2:
                        state_1 = log_res[2].get('state', {}).get('state_name', '')
                        state_1_time = log_res[2].get('gmt_created', '')
                        diff_date = time_trans(state_1_time) - time_trans(gmt_created_time)
                        diff_date_trans = time_trans2(diff_date)
                        timeout_flag = is_timeout_hours(diff_date)
                        output_suggestion = log_res[-2].get('suggestion', '')
                        second_value_list = [state_1, state_1_time, diff_date_trans, timeout_flag, output_suggestion, output_log]
                        second_key_list = ['IT处理', 'IT处理时间', 'IT一次处理耗时', '4小时超时','操作意见', '操作日志']
                out_dict[t_flowid].append(
                    pd.DataFrame([t_value_list + base_value_list + second_value_list],
                                    columns=t_key_list + base_key_list + second_key_list))

                # msg = str(ticket_result).encode('utf-8')
        pw = pd.ExcelWriter(file_path)
        for key, value in out_dict.items():
            out_data = pd.concat([pd.DataFrame(columns=key_dict[key])] + value)
            # if key == 1:
            #     out_data['IT一次处理耗时'] = (out_data['IT处理时间'].apply(time_trans) - out_data['创建时间'].apply(time_trans)).apply(time_trans2)
            # f.write(out_data.to_csv(quoting=1).encode('utf-8'))
            out_data.to_excel(pw, sheet_name=f'Flow{key}')

            # pw.save()
        pw.close()


    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        file_obj = TicketOutFile.objects.filter(id=file_obj_id).first()
        file_obj.is_deleted = True
        file_obj.save()

    else:
        file_obj = TicketOutFile.objects.filter(id=file_obj_id).first()
        file_obj.out_status = True
        file_obj.save()
        
output_data(ticket_ids, username, file_path, file_obj_id)   # 值由tasks传入