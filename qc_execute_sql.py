#!/usr/bin/env python
# coding=utf-8
import configparser as cparser
import mysql
from mysql import connector
import os
import sys
import time
import shutil

#  py文件当前路径
path = os.getcwd()

#  当前路径下的所有文件
files = os.listdir(path)


#  读取文件
def read_sql_file():

    #  定义保存sql的字典,文件名file作为key,文件内容作为value
    str_sql = {}

    print("当前目录{0}所有sql文件名:   \n".format(path))

    for file in files:
        if not os.path.isdir(file) and (".txt" in file or ".sql" in file):
            print(file)
            with open(file,'r', encoding='UTF-8') as f:
                sql = f.read()
            # sql = open(file,'r', encoding='UTF-8').read()
            str_sql[file] = sql

            f.close()
    return str_sql

#  执行sql
def execute_sql():


    #  获取py执行文件相同目录下,txt或者sql文件里面的sql
    sql_dict = read_sql_file()

    if len(sql_dict)==0:
        print("当前目录 {0} 不存在: sql或者txt后缀名的文件".format(path))
        return 0

    # 获取数据库名
    dbs_name =get_dbs()

    #  执行sql语句
    try:
        #  遍历sql字典,key是文件名,value是文件内容
        for key in sql_dict:

            # dbs_name是字符串类型,要用eval()方法转换成list类型
            dbs_yixinli =eval(dbs_name ["dbs_yixinli"])
            dbs_trade = eval(dbs_name ["dbs_trade"])
            dbs_xinli001_lx_college = eval(dbs_name ["dbs_xinli001_lx_college"])
            dbs_xinli001_cp = eval(dbs_name ["dbs_xinli001_cp"])


            print("\n"+"文件"+key+"包含的sql是:")
            print(sql_dict[key]+"\n")

            # 如果文件名包含trade,就选择trade数据库
            if  "trade" in key:
                dbs = dbs_trade

            # 如果文件名包含college,就选择college数据库
            elif "college" in key:
                dbs = dbs_xinli001_lx_college

            # 如果文件名包含cp,就选择cp数据库
            elif "cp" in key:
                dbs = dbs_xinli001_cp

            # 否则直接执行yixinli平台的数据库
            else:
                dbs = dbs_yixinli


            #  执行时传入的参数和数据库list的对应关系
            temp_list = ["qc1", "qc2", "qc3", "qc4", "qc5", "qc6"]

            #  外部传入的参数判断
            if len(sys.argv)<2:
                print("木有传输参数,将会在所有测试数据库执行sql脚本")
            else:
                if sys.argv[1] in temp_list:

                #  去除不要执行的数据库
                    dbs.pop(temp_list.index(sys.argv[1]))
                    print("本次执行将在以下数据库执行sql脚本")
                    print(dbs)
                else:
                    print("参数需要是其中的一个:")
                    print(temp_list)
                    break

            for db in dbs:
                #  获取数据操作游标
                cursor = conn.cursor()

                # 选择数据库
                cursor.execute("use " + db)
                print("选择数据库执行了:use {0} \n".format(db))

                # 执行sql
                cursor.execute(sql_dict[key], multi=True)
                print("sql语句执行了: {0}    \n".format(sql_dict[key]))




    except mysql.connector.Error as e:
        print('数据库错误:{}'.format(e))




    #  提交事务
    conn.commit()
    print("提交事务")

    #  关闭游标
    cursor.close()
    print("关闭数据库游标")

    # 修改sql文件的名字,并移动到当前目录的executed文件夹中
    rename_and_remove_sql_file(sql_dict)
    print("修改sql文件的名字,并移动到当前目录的executed文件夹中")

def get_mysql_configs():
    # 创建一个配置操作对象
    cf = cparser.ConfigParser()

    # 读取mysql配置文件
    cf.read("./mysql.conf")

    # dbs ={"xinli001":cf.get("databases",'dbs_yixinli')}

    # print(dbs)
    #
    # 把配置文件的内容赋值给变量
    configs = {"host": cf.get("mysql_conf", 'host'),
               "port": cf.get("mysql_conf", 'port'),
               "user": cf.get("mysql_conf", 'user'),
               "password": cf.get("mysql_conf", 'password')
               }
    return configs

def get_dbs():
    # 创建一个配置操作对象
    cf = cparser.ConfigParser()

    # 读取mysql配置文件
    cf.read("./dbs.conf")

    dbs ={"dbs_yixinli":cf.get("dbs",'dbs_yixinli'),
          "dbs_trade": cf.get("dbs", 'dbs_trade'),
          "dbs_xinli001_lx_college": cf.get("dbs", 'dbs_xinli001_lx_college'),
          "dbs_xinli001_cp": cf.get("dbs", 'dbs_xinli001_cp')
          }
    return  dbs

def rename_and_remove_sql_file(files):

    # 保存已经执行sql的目录
    executed_path = "./executed"

    # 如果保存sql的目录不存在,那就创建它
    if not os.path.exists(executed_path):
        os.mkdir(executed_path)

    # 获取当前目录下面包含sql和txt的文件
    # files = files

    for file in files:

        executed_time = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(time.time()))

        # 执行后的文件名
        executed_file = "executed_at_"+executed_time+"_"+file

        # 重命名已经执行的文件
        os.rename(file,executed_file)

        # 把已经执行的文件移动到保存sql的目录下
        shutil.move(executed_file,executed_path)


if __name__ == '__main__':

    # 连接数据库
    conn = mysql.connector.connect(**get_mysql_configs())

    # 调用直接sql的方法
    execute_sql()


    # rename_and_remove_sql_file()

    # 关闭数据库连接
    conn.close()




