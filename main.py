import copy
import os
import sqlite3
import subprocess
import time
import pandas as pd
import getopt
import sys

ROW_CMD = 'defaults write com.apple.dock springboard-rows -int '
COL_CMD = 'defaults write com.apple.dock springboard-columns -int '
RESET_CMD = 'defaults write com.apple.dock ResetLaunchPad -bool true; killall Dock'
PAD_CMD = 'echo $(getconf DARWIN_USER_DIR)/com.apple.dock.launchpad/db'


def sub(cmd: str, need_res: bool = False, sleep: float = 0.5):
    res = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).stdout.read()
    time.sleep(sleep)
    if need_res:
        return res


def set_size(row_num: int = 6, col_num: int = 9, do_reset: bool = True):
    sub(ROW_CMD + str(row_num))
    sub(COL_CMD + str(col_num))
    if do_reset:
        sub(RESET_CMD)


def update_db(db: str, do_save: bool = True):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute('SELECT * FROM sqlite_master;')
    objects = pd.DataFrame(c.fetchall())
    objects.columns = [m[0] for m in c.description]
    db_tables = []
    missing_data = []
    for table_name in objects[objects['type'] == 'table']['name']:
        c.execute('SELECT * FROM ' + table_name + ';')
        try:
            table_data = pd.DataFrame(c.fetchall())
            table_data.columns = [m[0] for m in c.description]
            db_tables.append((table_name, table_data))
        except ValueError:
            missing_data.append(table_name)
    items = copy.deepcopy(dict(db_tables)['items'])
    groups = copy.deepcopy(dict(db_tables)['groups'])
    apps = copy.deepcopy(dict(db_tables)['apps'])
    items_mod = copy.deepcopy(items)
    items_mod['item_id'] = items_mod['rowid']
    # 4 shows that it is an application, we're interested on the pages containing the apps:
    parents = sorted(list(items[items['type'] == 4]['parent_id'].drop_duplicates()))
    sort_by_parents = []
    for p in parents:
        under_page = items_mod[items_mod['parent_id'] == p]
        merged = pd.merge(under_page, apps)
        merged['Title'] = [f.upper() for f in merged['title']]
        new_merged = merged.sort_values('Title').reset_index().reset_index()
        new_merged['ordering'] = [x for x in new_merged['level_0']]
        sort_by_parents.append((p, new_merged[items.columns], new_merged))
    new_items = pd.concat(
        [pd.concat([n for (m, n, o) in sort_by_parents]),
            items[items['type'] != 4]]).sort_values('rowid')
    if do_save:
        new_items.to_sql('items', conn, if_exists='replace')
        time.sleep(3.0)
    conn.close()


def set_size_db(row_num: int = 6, col_num: int = 9, init_reset: bool = False, do_update: bool = False,
                do_save: bool = True):
    if init_reset:
        set_size(row_num, col_num)
    if do_update:
        db = os.path.join(str(sub(PAD_CMD, need_res=True).strip())[1:].replace('\'', ''), 'db')
        update_db(db, do_save)
    sub('killall Dock', need_res=True)


def main():
    args = sys.argv[1:]
    row_num = 6
    col_num = 9
    row_usage = '-r,--rows    N :\t number of rows to set in springboard\n'
    col_usage = '-c,--columns N :\t number of columns to set in springboard\n'
    try:
        opts, args = getopt.getopt(args, 'hr:c:', ['rows=', 'columns='])
        for opt, arg in opts:
            if opt == '-h':
                print('usage \n' + row_usage + col_usage)
                sys.exit()
            elif opt in ('-r', '--rows'):
                row_num = int(arg)
            elif opt in ('-c', '--columns'):
                col_num = int(arg)
        set_size_db(row_num, col_num, True, True)
    except getopt.GetoptError as e:
        print('GetOptError with msg:' + e.msg)
        print('usage \n' + row_usage + col_usage)


if __name__ == '__main__':
    main()
