# -*- coding: utf-8 -*-
# 脚本设置使用utf-8编码

from datetime import datetime
from glob import glob
import json
from enum import Enum
import os
import schema
import hashlib
import logging
import pandas as pd

SOURCE = 'TigerBot'
PRETRAIN_CUR_TITLE =''
OUTPUT_DIR = ''
CUR_TITLE = ''

# 配置日志记录
logging.basicConfig(
    filename='TigerBot_log_file.log',  # 指定日志文件的名称
    level=logging.INFO,  # 指定日志级别（INFO、WARNING、ERROR、CRITICAL等）
    format='%(asctime)s [%(levelname)s]: %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S'  # 日期和时间格式
)

# 定义一个枚举类
class Json_str(Enum):
    JSON_START = "{"
    JSON_END = '},'
    NONE = ''

# 处理 TigerBot 的数据
def tiger_bot_extract(f_path, output_file, type_str, max_size):
    if type_str == 'fine-tuning':
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        process_qa(f_path, output_file, max_size)
    elif type_str == 'domain':
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        process_domain_text(f_path, output_file, max_size)
    elif type_str == 'pretraining':
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        process_pretraining_text(f_path, output_file, max_size)
    else:
        logging.error(f"type: {type_str} not support!")

def process_qa(f_path, output_file, max_size):
    with open(f_path, 'r', encoding='utf-8') as f:
        # 读取第一行
        line = f.readline().strip('\n').replace('\ufeff', '')
        # 判断第一行是否为一个有效的json
        try:
            json_data = json.loads(line)
            # 按jsonl格式处理文件
            process_qa_file_common(f_path, output_file, max_size)
        except json.JSONDecodeError:
            line = f.readline()
            logging.warning("JSONDecodeError")
            logging.warning(line)
            # 按手动解析的方式处理文件
            process_qa_manual(f_path, output_file, max_size)

def process_qa_file_common(f_path, output_file, max_size):
    # 文件序号
    file_number = 1
    write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    with open(f_path, 'r', encoding='utf-8') as f:
        for line_number, line in enumerate(f, start=1):
            # 打印迭代信息
            logging.debug(f"Line {line_number}: {line}")
            this_line = line.strip('\n').replace('\ufeff', '')
            if process_qa_json_common(this_line, write_file):
                this_line = ''
            else:
                logging.error("common json parse error!")
                logging.error(f"Line {line_number}, error!")  # json检测失败
                logging.error(f"json str: {this_line}")
                break
            # 如果文件超过500M，就关闭文件，新建文件
            if write_file.tell() > max_size:
                write_file.close()
                file_number += 1
                write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')

def process_qa_manual(f_path, output_file, max_size):
    logging.warning("process_qa_manual")
    logging.warning(f"file: {f_path}")
    pass

def process_qa_json_common(json_str, write_file=None):
    # Check if str is a valid JSON
    try:
        json_data = json.loads(json_str)
        logging.debug(json_data)
        #用json_data的md5值作为id
        unique_id = hashlib.md5(json_str.encode('utf-8')).hexdigest()
        question_detail = "\"instruction\"\"input\""
        answer_detail = "\"output\""
        time = datetime.now().strftime("%Y%m%d")
        # 取出json中instruction对应的字符串，None转成空字符串
        question = json_data['instruction'] if json_data['instruction'] != None else ''
        question += json_data['input'] if json_data['input'] != None else ''
        answer = json_data['output'] if json_data['output'] != None else ''
        json_schema = schema.TigerBotQASchema(unique_id, question, answer, question_detail, answer_detail, unique_id, 1, f'\"{SOURCE}\"', '', time)
        # 生成json
        json_str = json_schema.to_json()
        write_file.write(json_str)
        write_file.write('\n')
        return True
    except json.JSONDecodeError:
        logging.error("JSONDecodeError")
        return False
    except KeyError:
        logging.error("KeyError")
        return False

def process_domain_text(f_path, output_file, max_size):
    with open(f_path, 'r', encoding='utf-8') as f:
        # 读取第一行
        line = f.readline().strip('\n').replace('\ufeff', '')
        # 判断第一行是否为一个有效的json
        try:
            json_data = json.loads(line)
            # 按jsonl格式处理文件
            process_text_file_common(f_path, output_file, max_size)
        except json.JSONDecodeError:
            line = f.readline()
            logging.warning("JSONDecodeError")
            logging.warning(line)
            # 按手动解析的方式处理文件
            process_text_file_manual(f_path, output_file, max_size)

def process_text_file_common(f_path, output_file, max_size):
    global CUR_TITLE
    # 文件序号
    file_number = 1
    json_datas = []
    write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    with open(f_path, 'r', encoding='utf-8') as f:
        for line_number, line in enumerate(f, start=1):
            # 打印迭代信息
            logging.debug(f"Line {line_number}: {line}")
            this_line = line.strip('\n').replace('\ufeff', '')
            try:
                json_data = json.loads(line)
                aaa = json_data['title']
            except json.JSONDecodeError or KeyError:
                logging.error("JSONDecodeError")
                logging.error(f"Line {line_number}, error!")
                exit()
            if CUR_TITLE != '' and CUR_TITLE != json_data['title']:
                if process_text_json_common(json_datas, write_file):
                    json_datas = []
                    json_datas.append(json_data)
                    CUR_TITLE = json_data['title']
                else:
                    logging.error("common json parse error!")
                    logging.error(f"Line {line_number}, error!")
                    exit()
            else:
                CUR_TITLE = json_data['title']
                json_datas.append(json_data)
            # 如果文件超过500M，就关闭文件，新建文件
            if write_file.tell() > max_size:
                write_file.close()
                file_number += 1
                write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    # 处理最后一批数据
        if process_text_json_common(json_datas, write_file):
            json_datas = []
            CUR_TITLE = ''
        else:
            logging.error("common json parse error!")
            logging.error(f"Line {line_number}, error!")
            exit()

def process_text_file_manual(f_path, output_file, max_size):
    """
    Process a text file manually, extracting JSON data and writing it to output files.

    Args:
        f_path (str): The path to the input text file.
        output_file (str): The base name of the output files.
        max_size (int): The maximum size (in bytes) of each output file.

    Returns:
        bool: True if the processing is successful, False otherwise.
    """
    # 文件序号
    file_number = 1
    write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    with open(f_path, 'r', encoding='utf-8') as f:
        buffer = ""
        json_len = 0
        json_str_flag = Json_str.NONE.value # 检测到json串时修改为对应状态，检测完成修改回NONE
        for line_number, line in enumerate(f, start=1):
            # 打印迭代信息
            logging.debug(f"Line {line_number}: {line}")
            # 如果json_len大于1000，就退出
            if json_len > 10000:
                logging.error(f"Line {line_number}, json len > 1000, exit!")  # json检测失败
                break
            this_line = line.strip()
            # 最后的逻辑放到前面，避免重复判断
            if json_str_flag == Json_str.JSON_START.value:
                if this_line == Json_str.JSON_END.value:
                    logging.debug(f"Line {line_number}, json end!")  # json解析开始
                    buffer += "}"
                    # 重置状态
                    json_str_flag = Json_str.NONE.value
                    json_len = 0
                    logging.debug(f"Line {line_number}, json str: {buffer}")
                    json_data = json.loads(buffer)
                    json_datas = []
                    json_datas.append(json_data)
                    if process_text_json_common(json_datas, write_file):
                        buffer = ''
                        json_len += 1
                        # 如果文件超过500M，就关闭文件，新建文件
                        if write_file.tell() > max_size:
                            write_file.close()
                            file_number += 1
                            write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
                        continue
                    else:
                        logging.error(f"Line {line_number}, error!")  # json检测失败
                        logging.error(f"parse stage: {json_str_flag}, json str: {buffer}")
                        break
                else:
                    buffer += this_line
                    continue
            if json_str_flag == Json_str.NONE.value:
                if this_line == "[":
                    logging.debug(f"Line {line_number}, start of text!")  # json检测文件开始
                    continue
                elif this_line == "]":
                    logging.debug(f"Line {line_number}, end of text!")  # json检测到末尾了
                    break 
                elif this_line != "{":
                    logging.error(f"Line {line_number}, error!")  # json检测失败
                    break
                elif this_line == Json_str.JSON_START.value:
                    logging.debug(f"Line {line_number}, start parsing json!")  # json解析开始
                    json_str_flag = Json_str.JSON_START.value
                    buffer += this_line
                    continue
                else:
                    logging.error(f"Line {line_number}, error!")  # json检测失败
                    logging.error(f"parse stage: {json_str_flag}, json str: {buffer}")
                    break
    return True

def process_text_json_common(json_datas, write_file=None):
    global CUR_TITLE
    # 计算json_datas的总大小
    size = 0
    paragraphs = []
    line_number = 0
    time = datetime.now().strftime("%Y%m%d")
    for json_data in json_datas:
        logging.debug(json_data)
        json_str = json.dumps(json_data, ensure_ascii=False).encode('utf-8')
        size += len(json_str)
        line_number += 1
        md5 = hashlib.md5(json_str).hexdigest()
        title = CUR_TITLE
        content = json_data['content']
        extended_field = ''
        # 如果存在publishTime的key，就取出来
        if 'publishTime' in json_data:
            extended_field += ", \"publishTime\": \""+ json_data['publishTime'] +"\""
        # 如果存在chapter1的key，就取出来
        if 'chapter1' in json_data:
            extended_field += ", \"chapter1\": \""+ json_data['chapter1'] +"\""
        # 如果存在chapter1的key，就取出来
        if 'type' in json_data:
            extended_field += ", \"type\": \""+ json_data['type'] +"\""
        if 'wiki_id' in json_data:
            extended_field += ", \"wiki_id\": \""+ str(json_data['wiki_id']) +"\""
        if 'url' in json_data:
            extended_field += ", \"url\": \""+ json_data['url'] +"\""
            # 使用url作为title
            CUR_TITLE = json_data['url']
        para_json_schema = schema.TigerBotTextParagraphSchema(line_number, md5, title, '', content, extended_field).to_json()
        paragraphs.append(para_json_schema)
    json_schema = schema.TigerBotTextSchema(CUR_TITLE, size, '', paragraphs, time)
    # 生成json
    json_str = json_schema.to_json()
    write_file.write(json_str)
    write_file.write('\n')
    return True

def process_pretraining_text(file_path, output_file, max_size):
    global PRETRAIN_CUR_TITLE
    parquet_datas = []
    # 文件序号
    file_number = 1
    write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    chunk = pd.read_parquet(file_path)
    for index, row in chunk.iterrows():
        logging.debug(f"Line {index}: {row}")
        if PRETRAIN_CUR_TITLE != '' and PRETRAIN_CUR_TITLE != row['title']:
            if process_text_parquet_common(parquet_datas, write_file):
                parquet_datas = []
                parquet_datas.append(row)
                PRETRAIN_CUR_TITLE = row['title']
            else:
                logging.error("process_pretraining_text parse error!")
                logging.error(f"Line {index}, error!")
                exit()
        else:
            PRETRAIN_CUR_TITLE = row['title']
            parquet_datas.append(row)
                    # 如果文件超过500M，就关闭文件，新建文件
        if write_file.tell() > max_size:
            write_file.close()
            file_number += 1
            write_file = open(OUTPUT_DIR + f'{output_file}_{file_number:02}.jsonl', 'w', encoding='utf-8')
    # 处理最后一批数据
    if process_text_parquet_common(parquet_datas, write_file):
        parquet_datas = []
        PRETRAIN_CUR_TITLE = ''
    else:
        logging.error("process_pretraining_text parse error!")
        logging.error(f"Last line, error!")
        exit()

def process_text_parquet_common(parquet_datas, write_file=None):
    # 计算json_datas的总大小
    size = 0
    paragraphs = []
    line_number = 0
    time = datetime.now().strftime("%Y%m%d")
    for parquet_data in parquet_datas:
        str_data = str(parquet_data)
        size += len(str_data)
        line_number += 1
        md5 = hashlib.md5(str_data.encode('utf-8')).hexdigest()
        title = PRETRAIN_CUR_TITLE
        content = parquet_data['content']
        extended_field = ''
        # 如果存在dataType的key，就取出来
        if parquet_data['dataType']:
            extended_field += ", \"dataType\": \""+ parquet_data['dataType'] +"\""
        # 如果存在uniqueKey的key，就取出来
        if parquet_data['uniqueKey']:
            extended_field += ", \"uniqueKey\": \""+ parquet_data['uniqueKey'] +"\""
        # 如果存在titleUkey的key，就取出来
        if parquet_data['titleUkey']:
            extended_field += ", \"titleUkey\": \""+ parquet_data['titleUkey'] +"\""
        # 如果存在id的key，就取出来
        if parquet_data['id']:
            extended_field += ", \"id\": \""+ str(parquet_data['id']) +"\""
        para_json_schema = schema.TigerBotTextParagraphSchema(line_number, md5, title, '', content, extended_field).to_json()
        paragraphs.append(para_json_schema)
    json_schema = schema.TigerBotTextSchema(PRETRAIN_CUR_TITLE, size, '', paragraphs, time)
    # 生成json
    json_str = json_schema.to_json()
    write_file.write(json_str)
    write_file.write('\n')
    return True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse TigerBot data.")
    parser.add_argument("source_files",type=str, default="final_data_sample_230706test.json", help="文件名或者目录名")
    parser.add_argument("dest_dir",type=str, default="final_data_sample_230706test", help="文件名或者目录名")
    parser.add_argument("-s","--max_size",type=int,default=500 * 1024 * 1024,help="max chunk size")
    # type的类型
    # fine-tuning: 用于fine-tuning的数据
    # domain: 用于domain的数据
    # pretraining: 用于pretraining的数据
    parser.add_argument("-m","--type",type=str,default='fine-tuning',help="multi model parse")
    args = parser.parse_args()
    # 当前时间
    cur_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_paths = []
    OUTPUT_DIR = args.dest_dir

    # 检查文件是否存在
    if os.path.isfile(args.source_files):
        file_paths.append(args.source_files)
    elif os.path.isdir(args.source_files):
        for f_path in sorted(glob(f"{args.source_files}/**/*.json*", recursive=True)):
            if not os.path.isfile(f_path):
                # 跳过子目录并打印日志
                logging.warning(f"Skipping subdirectory {f_path}")
                continue
            file_paths.append(f_path)
        for f_path in sorted(glob(f"{args.source_files}/**/*.parquet", recursive=True)):
            if not os.path.isfile(f_path):
                # 跳过子目录并打印日志
                logging.warning(f"Skipping subdirectory {f_path}")
                continue
            file_paths.append(f_path)
    # 打印处理清单
    logging.info(f"Processing {len(file_paths)} files")
    logging.info(f"Processing {args.type} data")
    for f_path in file_paths:
        logging.info(f"Processing {f_path}")
        f_prefix_name = os.path.basename(f_path).split('.')[0]
        output_file_prefix = f'{SOURCE}_{args.type}_{f_prefix_name}_{cur_time}'
        # 调用函数来处理 JSON 文件，默认从第1行开始读取
        tiger_bot_extract(f_path, output_file_prefix, args.type, args.max_size)
 