from functools import wraps
from service.account.account_base_service import account_base_service_ins
from service.format_response import api_response


def manage_permission_check(permission=None):
    """
    manage permission check decorator
    :param permission:
    :return:
    """
    def decorator(func):
        @wraps(func)
        def _deco(view_class, request, *args, **kwargs):
            username = request.META.get('HTTP_USERNAME')
            if permission == 'admin':
                flag, result = account_base_service_ins.admin_permission_check(username)
                if flag is False:
                    return api_response(-1, 'has no permission:{}'.format(result), {})
            elif permission == 'workflow_admin':
                flag, result = account_base_service_ins.workflow_admin_permission_check(username)
                if flag is False:
                    return api_response(-1, 'has no permission:{}'.format(result), {})
            else:
                flag_admin, result1 = account_base_service_ins.admin_permission_check(username)
                flag_flow, result2 = account_base_service_ins.workflow_admin_permission_check(username)
                flag_user, result3 = account_base_service_ins.get_user_role_id_list(username)
                if not (flag_admin or flag_flow or flag_user):
                    return api_response(-1, 'has no permission', {})
                elif flag_user and not (1 in result3 or 41 in result3 or 52 in result3):
                    return api_response(-1, '无下载权限，请联系管理员配置'.encode('utf-8'), {})
            return func(view_class, request, *args, **kwargs)

        return _deco
    return decorator

