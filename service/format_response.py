import json

from django.http import HttpResponse, StreamingHttpResponse


def api_response(code, msg='', data=''):
    """
    格式化返回
    :param code:
    :param msg:
    :param data:
    :return:
    """
    return HttpResponse(json.dumps(dict(code=code, data=data, msg=msg)), content_type="application/json")


def api_fileresponse(data, name, content_type="application/octet-stream"):
    res = StreamingHttpResponse(data, content_type=content_type)
    res['Content-Disposition'] = 'attachment;filename="{}"'.format(name)
    return res
