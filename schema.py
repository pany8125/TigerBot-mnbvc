import json
from datetime import datetime

class TigerBotQASchema:

    def __init__(self, id, question, answer, question_detail, answer_detail, session, round_number, model, other_field):
        self.id = id
        self.question = question
        self.answer = answer
        self.source = "TigerBot"
        self.create_time = datetime.now().strftime("%Y%m%d %H:%M:%S")
        self.question_detail = question_detail 
        self.answer_detail = answer_detail 
        # 扩展字段
        self.extended_field = "{\"会话\": " + session + ", \"多轮序号\": " + str(round_number) + ", \"解析模型\": " + model + other_field + "}"

    def to_json(self):
        data = {
            "id": self.id,
            "问": self.question,
            "答": self.answer,
            "来源": self.source,
            "元数据": {
                "create_time": self.create_time,
                "问题明细": self.question_detail,
                "回答明细": self.answer_detail,
                "扩展字段": self.extended_field
            }
        }
        # jsonl的库处理下
        # 扩展字段直接json dump
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

class TigerBotTextSchema:

    def __init__(self, file_name, file_size, extended_field, paragraps):
        self.file_name = file_name
        self.file_size = file_size
        self.extended_field = extended_field
        self.paragrap = paragraps

    def to_json(self):
        data = {
                "文件名": self.file_name,
                "是否待查文件": False,
                "是否重复文件": False,
                "文件大小": self.file_size,
                "simhash": 0,
                "最长段落长度": 0,
                "段落数": 0,
                "去重段落数": 0,
                "低质量段落数": 0,
                '扩展字段': self.extended_field,
                "段落": self.paragrap
            }
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

# TigerBotText段落Schema
class TigerBotTextParagraphSchema:

    def __init__(self, line_number, md5, title, author, content, extended_field):
        self.line_number = line_number
        self.md5 = md5
        self.title = title
        self.author = author
        self.content = content
        self.extended_field = extended_field

    def to_json(self):
        data = {
                    '行号': self.line_number,
                    '是否重复': False,
                    '是否跨文件重复': False,
                    'md5': self.md5, #整行json的md5
                    '标题': self.title, # 对应title
                    '作者': self.author, # 对应author
                    '内容': self.content, # 对应content
                    '扩展字段': self.extended_field # 对应content
                }
        # return json.dumps(data, separators=(",", ":"), ensure_ascii=False)
        return data


# paragraph = []
# paragraph.append(TigerBotTextParagraphSchema(1, "md5", "title", "author", "content", "extended_field").to_json())
# paragraph.append(TigerBotTextParagraphSchema(2, "md5", "title", "author", "content", "extended_field").to_json())

# data = {
#         "低质量段落数": 0,
#         "段落": paragraph
#     }
# str = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
# print(str)