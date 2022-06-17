import traceback
from apps.ticket.models import TicketRecord
from service.ticket.ticket_base_service import ticket_base_service_ins
from service.account.account_base_service import account_base_service_ins
import pandas as pd
import logging

logger = logging.getLogger('django')

"""
1.因为使用execfile/exec执行脚本，脚本中会跟随celery的执行环境
2.ticket_id和action_from参数会通过调用的时候传递过来，可以直接使用.可以使用ticket_id获取ticket相关的信息
3.因为使用execfile/exec执行脚本, 不得使用if __name__ == '__main__'
4.本脚本场景为服务器权限申请，工单中有自定义字段:host_ip
"""
f_path = r".\media\ticket_temp\DICT工单项目文档.xlsx"

def load_file(f_path):
    return pd.read_excel(f_path, dtype={'县市CT管理员电话': str,
                                        '县市DICT管理员电话':str,
                                        '一线质保人员电话':str,
                                        '一线维护人员电话':str})

def demo_script_call():
    # 获取工单信息ip地址信息
    ticket_base_service_ins.new_ticket(data_dict, 'loonflow')
    username, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'creator')  # ticket_id会通过exec传过来
    file_path, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'file_update')  # ticket_id会通过exec传过来
    # 你也可以获取工单的其他字段信息，包括自定义字段的值。根据获取的值做后续处理
    with open('D:\Python\loonflow_add\j.json', 'w') as c:
        c.write(str(locals()))
    print(username, file_path)
    return True, ''


def get_user_data():
    """
    获取人员信息接口
    Returns:
        _type_: _description_
    """
    v_dict = {}
    page_num = 200
    flag, result = account_base_service_ins.get_user_list('', 1, page_num, simple=True)
    if flag is not False:
        total = result.get('paginator_info').get('total')
        if total > page_num:
            flag, result = account_base_service_ins.get_user_list('', 1, total, simple=True)
        value = result.get('user_result_object_format_list')
        for v_d in value:
            v_dict.update({f"{v_d['alias']}_{v_d['phone']}": v_d['username']})
    return v_dict


def update_principal(request_data_dict, v_dict, key_list):
    ret_msg = ''
    for select_key in key_list:
        if request_data_dict.get(select_key):
            principal_value = request_data_dict.get(select_key)
            principal_phone = request_data_dict.get(f'{select_key}_phone').replace(' ','')
            principal_phone_data = principal_phone.split('/')
            t = []
            username = ''
            for index, n in enumerate(principal_value.split('/')):
                if len(principal_phone_data) > index:
                    un = v_dict.get(f'{n}_{principal_phone_data[index][7:]}', '')
                    if un != '':
                        t.append(un)
                username = ','.join(t)
                
            request_data_dict.update({f'{select_key}_name': username})
            
            if username == '':
                ret_msg += f'无{select_key}信息；'
    return request_data_dict, ret_msg

def run():
    ret_msg = ''
    s_data = ''
    r_dict = {}
    search_value = ''
    search_key = ''
    request_data_dict = {'project_type': '',
                         'it_principal':'',
                         'it_principal_name':'',
                         'it_principal2':'',
                         'it_principal2_name':'',
                         'ct_principal':'',
                         'ct_principal_name':'',}
    # project_principal, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id,
    #                                                                         'project_principal')  # ticket_id会通过exec传过来
    project_code, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id,
                                                                            'project_code')  # ticket_id会通过exec传过来
    project, msg2 = ticket_base_service_ins.get_ticket_field_value(ticket_id,
                                                                            'project')  # ticket_id会通过exec传过来
    project_code = msg.get('value', '')
    project = msg2.get('value', '')
    if project_code:
        search_value = project_code
        search_key = 'project_code'
    elif project:
        search_value = project
        search_key = 'project'
    
    if search_key != '':
        try:
            df_data = load_file(f_path)
            ct_principal_data = df_data[['县市', '项目名称', '项目编号', '项目类型', '所属客户', '县市DICT管理员', '县市DICT管理员电话', '县市CT管理员', '县市CT管理员电话']].copy()
            ct_principal_data.columns = ['county', 'project', 'project_code', 'project_type', 'belong_customer', 'dict_manager', 'dict_manager_phone', 'ct_principal', 'ct_principal_phone']
            it_data = df_data[['一线维护人员', '一线维护人员电话']].copy()
            it_data.columns = ['it_principal', 'it_principal_phone']
            it2_data = df_data[['一线质保人员', '一线质保人员电话']].copy()
            it2_data.columns = ['it_principal2', 'it_principal2_phone']
            proncipal_data = pd.concat([ct_principal_data, it_data, it2_data], axis=True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(e)
            ret_msg += '项目人员表信息异常；'
        else:
            s_data = proncipal_data[proncipal_data[search_key] == search_value]
        #     logger.info(s_data)
        # logger.info(search_key)
        # logger.info(search_value)
        if len(s_data) > 0:
            # 获取平台用户记录
            v_dict = get_user_data()
            # 多个项目同名时的处理
            key_list = ['it_principal', 'it_principal2', 'ct_principal', 'dict_manager']
            
            if len(s_data) > 1:
                r_dict = {}
                for i in range(len(s_data)):
                    temp = s_data.iloc[i,:].to_dict()
                    
                    if len(r_dict) != 0:
                        for i in key_list:
                            principal_key = i
                            principal_phone_key = f'{i}_phone'
                            if temp[principal_key] not in r_dict[principal_key]:
                                r_dict[principal_key] = r_dict[principal_key] + '/' + temp[principal_key]
                                r_dict[principal_phone_key] = str(r_dict[principal_phone_key]) + '/' + str(temp[principal_phone_key])
                        
                    else:
                        r_dict = temp
            else:
                r_dict = s_data.iloc[0,:].to_dict()

            request_data_dict.update({'project':r_dict.get('project'),
                                      'project_code':r_dict.get('project_code'),
                                    'project_type':r_dict.get('project_type'),
                                    'belong_customer':r_dict.get('belong_customer')
                                    })
            for i in key_list:
                principal_key = i
                principal_phone_key = f'{i}_phone'
                request_data_dict.update({
                     principal_key:r_dict.get(principal_key),
                     principal_phone_key:str(r_dict.get(principal_phone_key)),
                     })
            request_data_dict, rets = update_principal(request_data_dict, v_dict, key_list)
            ret_msg += rets
        else:
            ret_msg += '项目信息未匹配；'
    
    # key_list = ['it_principal', 'it_principal2', 'dict_manager']
    # request_data_dict = update_principal(request_data_dict, v_dict, key_list)
    

    # county_name, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'county_name')  # ticket_id会通过exec传过来
    # if county_name:
    #     county_name_value = msg.get('value').strip().strip('区市县')
    #     if county_name_value in ['武义', '东阳', '永康', '磐安', '婺城',
    #                              '浦江', '金东', '义乌', '兰溪', '方大',
    #                              '宏信', '智杰', '众网', '汇邦', '安诚',
    #                              '威力克', '科友', '至力', '天博']:
    #         request_data_dict.update(
    #             {'ct_principal': f'CT维护-{county_name_value}', 'county_manager': f'GA县市管理员-{county_name_value}'})
    # if request_data_dict.get('ct_principal', '') == '':
    #     ret_msg += 'CT（或县市）信息错误；'
    logger.debug(ret_msg)
    result, msg = ticket_base_service_ins.update_ticket_field_value(ticket_id, request_data_dict)
    print(ret_msg)
    return True, ret_msg


run()
