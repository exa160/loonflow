import json
import os

from django.http import HttpResponse, FileResponse
# from django.utils.encoding import escape_uri_path

def api_response(code, msg='', data=''):
    """
    格式化返回
    :param code:
    :param msg:
    :param data:
    :return:
    """
    return HttpResponse(json.dumps(dict(code=code, data=data, msg=msg)), content_type="application/json")


def api_fileresponse(data):
    res = FileResponse(data, as_attachment=True)
    res.set_headers(data)
    res['Content-Length'] = os.path.getsize(data.name)
    return res

