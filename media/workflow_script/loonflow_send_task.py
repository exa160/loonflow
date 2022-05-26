import traceback
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
f_path = r".\media\ticket_file\全量点位信息.xlsx"


def load_file(f_path):
    return pd.read_excel(f_path, sheet_name=None, dtype={'一线维护人员联系方式': object})


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
    """
    更新处理人状态

    Args:
        request_data_dict (dict): 包含处理人状态信息的字典
        v_dict (dict): 人员信息的字典
        key_list (list): 需要增加的关键字

    Returns:
        dict, str: request_data_dict, result
    """
    ret_msg = ''
    for select_key in key_list:
        if request_data_dict.get(select_key):
            principal_value = request_data_dict.get(select_key)
            principal_phone = request_data_dict.get(f'{select_key}_phone').replace(' ','')
            principal_phone_data = principal_phone.split(',')
            t = []
            username = ''
            for index, n in enumerate(principal_value.split(',')):
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
    v_dict = get_user_data()
    r_dict = {}
    request_data_dict = {'project':'',
                         'project_status':'',
                         'ct_principal': '',
                         'delivered':'',
                         'county_manager': '',
                         'maintenance_unit':'',
                         'project_principal':'',
                         'project_principal_name': ''}
    # project_principal, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id,
    #                                                                         'project_principal')  # ticket_id会通过exec传过来
    camera_id, msg2 = ticket_base_service_ins.get_ticket_field_value(ticket_id,
                                                                            'camera_id')  # ticket_id会通过exec传过来
    if camera_id:
        camera_id_value = msg2.get('value').strip()
        try:
            df_data = load_file(f_path)
            df_data = pd.concat([i[['国标编码','区县','所属项目','维护单位','是否交维','是否到期','一线维护人员','一线维护人员联系方式']] for i in list(df_data.values())])
            df_data.columns = ['国标编码','区县','所属项目','维护单位','是否交维','是否到期','project_principal','project_principal_phone']
            # df_data['project_principal_phone'] = df_data['project_principal_phone'].astype('object').fillna('NA').apply(lambda x:str(int(x)))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(e)
            ret_msg += '项目人员表信息异常；'
        else:
            s_data = df_data[df_data['国标编码'] == camera_id_value]
        if len(s_data) > 0:
            r_dict  = s_data.iloc[0,:].to_dict()
            request_data_dict.update({'project':r_dict.get('所属项目'),
                                      'county_name':r_dict.get('区县'),
                         'project_status':r_dict.get('是否到期'),
                         'delivered':r_dict.get('是否交维'),
                         'maintenance_unit':r_dict.get('维护单位'),
                         })
        
        key_list = ['project_principal']
        for i in key_list:
            principal_key = i
            principal_phone_key = f'{i}_phone'
            request_data_dict.update({
                    principal_key:r_dict.get(principal_key),
                    principal_phone_key:str(r_dict.get(principal_phone_key)),
                    })
        logger.info(request_data_dict)
        request_data_dict, rets = update_principal(request_data_dict, v_dict, key_list)
        logger.info(request_data_dict)
        
            
    if request_data_dict.get('project_principal_name', '') == '':
        ret_msg += '无人员信息；'

    county_name, msg = ticket_base_service_ins.get_ticket_field_value(ticket_id, 'county_name')  # ticket_id会通过exec传过来
    if county_name:
        county_name_value = msg.get('value').strip().strip('区市县')
        county_name_value2 = request_data_dict.get('county_name').strip().strip('区市县')
        if county_name_value2 != county_name_value:
            ret_msg += '县市信息填写错误，已修正；'
            county_name_value = county_name_value2
        if county_name_value in ['武义', '东阳', '永康', '磐安', '婺城',
                                 '浦江', '金东', '义乌', '兰溪', '方大',
                                 '宏信', '智杰', '众网', '汇邦', '安诚',
                                 '威力克', '科友', '至力', '天博']:
            request_data_dict.update(
                {'ct_principal': f'CT维护-{county_name_value}', 'county_manager': f'GA县市管理员-{county_name_value}'})
    if request_data_dict.get('ct_principal', '') == '':
        ret_msg += 'CT（或县市）信息错误；'
    result, msg = ticket_base_service_ins.update_ticket_field_value(ticket_id, request_data_dict)
    print(ret_msg)
    return True, ret_msg


run()
