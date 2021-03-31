"""
解析go-ycsb的输出，标准化成dict格式
"""
import re


class YCSBResolver(object):
    def __init__(self, filename):
        with open(filename) as fi:
            self.content = fi.read()

    def resolve_elements(self):
        """
        解析YCSB输出参数，输出到dict中

        :return:
        {
            'operationcount': '10000',
            'insertproportion': '0',
            ...
        }
        """
        match = re.findall('\"(.*)\"\=\"(.*)\"', self.content)
        ycsb_elements = {item[0]: item[1] for item in match}
        return ycsb_elements

    def resolve_results(self):
        """
        解析YCSB输出结果, 输出到dict中

        :return:
        {
            'READ': {
                'Takes(s)': '',
                'Count': '',
                'OPS': '',
                ...
            },
            'UPDATE': {
                'Takes(s)': '',
                'Count': '',
                'OPS': '',
                ...
            }
        }
        """
        ycsb_results = {}
        search = re.search(r'Run finished.*\n?(.*)\n?(.*)', self.content)
        str1 = search.group(1)
        str2 = search.group(2)

        tmp_list1 = str1.split('-')
        tmp_dict1 = {item.split(':')[0].strip(): item.split(':')[1].strip() for item in tmp_list1[1].split(',')}
        ycsb_results[tmp_list1[0].strip()] = tmp_dict1

        tmp_list2 = str2.split('-')
        tmp_dict2 = {item.split(':')[0].strip(): item.split(':')[1].strip() for item in tmp_list2[1].split(',')}
        ycsb_results[tmp_list2[0].strip()] = tmp_dict2
        return ycsb_results




