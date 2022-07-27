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

DEFAULT_STUFF_ORDER = [
    'App Store', 'Safari', 'Mail', 'Contacts', 'Calendar', 'Reminders', 'Notes', 'FaceTime', 'Messages', 'Maps',
    'Find My', 'Photo Booth', 'Photos', 'Preview', 'Music', 'Podcasts', 'TV', 'Voice Memos', 'GarageBand', 'iMovie',
    'Numbers', 'Keynote', 'Pages', 'News', 'Stocks', 'Books', 'Dictionary', 'Calculator', 'Home', 'Siri',
    'Mission Control', 'System Preferences', 'Other', 'Games'
]

NAMED_NOT_REORDER = {'Other'}

TYPE_MAP = {1: 'root', 2: 'group', 3: 'holding', 4: 'apps'}


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


def get_parent_graph(items):
    items_dict = dict(zip(items['rowid'], items['parent_id']))
    items_parent_dict = {}
    for i in items_dict:
        j = i
        parent_chain = []
        while j in items_dict.keys():
            parent_chain.append(j)
            j = items_dict[j]
        items_parent_dict[i] = parent_chain
    return items_parent_dict


def df_to_dict(df, col1, col2):
    return dict(df[[col1, col2]].groupby(col1).sum()[col2])


def calc_ordering_old(apps: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
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
    return new_items


def calc_ordering(apps: pd.DataFrame, items: pd.DataFrame, groups: pd.DataFrame, row_num: int,
                  col_num: int) -> pd.DataFrame:
    # number of apps per page
    num_per_page = row_num * col_num
    d_cols = ['item_id', 'title']
    # app and item titles
    dict_title = df_to_dict(pd.concat([apps[d_cols], groups[d_cols]]), d_cols[0], d_cols[1])
    df_cp = copy.deepcopy(items)
    # map types and titles
    df_cp['category'] = df_cp['type'].apply(lambda x: TYPE_MAP[x])
    df_cp['title'] = df_cp['rowid'].apply(lambda x: dict_title[x])
    df_cp['title_upper'] = df_cp['title'].apply(lambda x: x.upper() if type(x) == str else x)
    # get defaults:
    df_cp['is_default'] = df_cp['title'].apply(lambda x: x in DEFAULT_STUFF_ORDER)
    # what we need to order
    df_cp['to_order'] = df_cp['category'].isin(['apps', 'group'])
    # get holding pages:
    # it's an unnamed holding page
    # that has children:
    holding_cond1 = (df_cp['category'] == 'holding') & df_cp['rowid'].isin(set(df_cp[df_cp['to_order']]['parent_id']))
    # but its parents are not to be ordered:
    holding_cond2 = df_cp['parent_id'].isin(set(df_cp[df_cp['to_order']]['rowid']))
    main_page = df_to_dict(df_cp[holding_cond1 & ~holding_cond2], 'ordering', 'rowid')
    main_page = dict([(idx, main_page[key]) for (idx, key) in enumerate(sorted(list(main_page.keys())))])
    # minor_holding_pages = df_cp[holding_cond1 & holding_cond2]
    # now find those apps which are under a named holding page - we leave them as they are
    # step through the rowid - parent_id pairs
    parents = get_parent_graph(df_cp)
    named_id = df_cp[df_cp['category'].isin(['group'])]['rowid']
    df_cp['is_named_child'] = df_cp['rowid'].apply(lambda x: any([n in parents[x] for n in named_id]))
    # exclude all those which are under the not reorder named group:
    exclude_rowid = set(df_cp[df_cp['category'].isin(['group']) & df_cp['title'].isin(NAMED_NOT_REORDER)]['rowid'])
    df_cp['is_under_exclude'] = df_cp['rowid'].apply(lambda x: any([e in parents[x] for e in exclude_rowid]))

    # group 1 - defaults
    defaults = copy.deepcopy(df_cp[df_cp['is_default']])
    # get raw order based on the default list
    defaults['raw_order'] = defaults['title'].apply(lambda x: DEFAULT_STUFF_ORDER.index(x))
    # some of these may have been deleted, so remap continuously:
    raw_order_remap = dict(enumerate(sorted((list(defaults['raw_order'])))))
    defaults['raw_order_remap'] = defaults['raw_order'].apply(lambda x: raw_order_remap[x])
    # now get the page id
    defaults['page'] = defaults['raw_order_remap'].apply(lambda x: x // num_per_page)
    defaults['page_id'] = defaults['page'].apply(lambda x: main_page[x])
    # then the order on this page
    defaults['order'] = defaults['raw_order_remap'].apply(lambda x: x % num_per_page)
    default_page_occupied = defaults['page'].max() + 1

    # group2 - apps not under groups
    own_apps = copy.deepcopy(df_cp[~df_cp['is_default'] & df_cp['to_order'] & ~df_cp['is_named_child']])
    # sort by title
    own_apps = own_apps.sort_values(['title_upper'])
    # get the order
    own_apps['raw_order_remap'] = [idx for idx, _ in enumerate(own_apps['rowid'])]
    # get the page, considering that the defaults have taken up place:
    own_apps['page'] = own_apps['raw_order_remap'].apply(lambda x: x // num_per_page + default_page_occupied)
    if max(own_apps['page']) > max(main_page.keys()):
        raise ValueError('more main pages needed than available... do a hack with smaller pages to get uuid?')
    own_apps['page_id'] = own_apps['page'].apply(lambda x: main_page[x])
    own_apps['order'] = own_apps['raw_order_remap'].apply(lambda x: x % num_per_page)

    # group3 - apps not defaulted and under groups to be kept at the same level
    group_apps = copy.deepcopy(
        df_cp[~df_cp['is_default'] & df_cp['to_order'] & df_cp['is_named_child'] & ~df_cp['is_under_exclude']])
    # group by named parent:
    group_apps['named_parent'] = group_apps['rowid'].apply(
        lambda x: [parent for parent in parents[x] if parent in list(named_id)][0])
    # for each named parent look at the children
    parts = []
    for p in set(group_apps['named_parent']):
        part = copy.deepcopy(group_apps[group_apps['named_parent'] == p])
        # sort by title
        part = part.sort_values('title_upper')
        # get the relevant pages:
        page_map = df_to_dict(df_cp[df_cp['rowid'].isin(set(part['parent_id']))], 'ordering', 'rowid')
        # again the same:
        part['raw_order_remap'] = [idx for idx, _ in enumerate(part['rowid'])]
        part['page'] = part['raw_order_remap'].apply(lambda x: x // num_per_page)
        part['page_id'] = part['page'].apply(lambda x: page_map[x])
        part['order'] = part['raw_order_remap'].apply(lambda x: x % num_per_page)
        parts.append(part)
    grouped_apps = pd.concat(parts)
    all_remapped = pd.concat([defaults, own_apps, grouped_apps])
    all_parent = df_to_dict(all_remapped, 'rowid', 'page_id')
    all_order = df_to_dict(all_remapped, 'rowid', 'order')
    df_final = copy.deepcopy(df_cp)
    df_final['parent_id'] = [all_parent.get(id_, base) for id_, base in zip(df_final['rowid'], df_final['parent_id'])]
    df_final['ordering'] = [all_order.get(id_, base) for id_, base in zip(df_final['rowid'], df_final['ordering'])]
    return df_final


def get_data_from_conn(conn):
    c = conn.cursor()
    c.execute('SELECT * FROM sqlite_master;')
    objects = pd.DataFrame(c.fetchall())
    objects.columns = [m[0] for m in c.description]
    db_tables = []
    missing_data = []
    for table_name in objects[objects['type'] == 'table']['name']:
        c.execute('SELECT * FROM ' + table_name + ';')
        try:
            table_raw_data = c.fetchall()
            table_data = pd.DataFrame(table_raw_data)
            table_data.columns = [m[0] for m in c.description]
            db_tables.append((table_name, table_data))
        except ValueError:
            missing_data.append(table_name)
    items = copy.deepcopy(dict(db_tables)['items'])
    groups = copy.deepcopy(dict(db_tables)['groups'])
    apps = copy.deepcopy(dict(db_tables)['apps'])
    return items, groups, apps


def update_db(db: str, do_save: bool = True, do_new: bool = True, row_num: int = 6, col_num: int = 9):
    conn = sqlite3.connect(db)
    items, groups, apps = get_data_from_conn(conn)
    if do_new:
        new_items = calc_ordering(apps, items, groups, row_num, col_num)[items.columns]
    else:
        new_items = calc_ordering_old(apps, items)
    if do_save:
        new_items.to_sql('items', conn, index=False, if_exists='replace')
        new_items.to_pickle(str(do_new) + 'new_items.pkl')
        # items.to_pickle('items.pkl')
        time.sleep(3.0)
    conn.close()


def set_size_db(row_num: int = 6, col_num: int = 9, init_reset: bool = False, do_update: bool = False,
                do_save: bool = True, do_new: bool = True):
    if init_reset:
        set_size(row_num, col_num)
    if do_update:
        db = os.path.join(str(sub(PAD_CMD, need_res=True).strip())[1:].replace('\'', ''), 'db')
        update_db(db, do_save, do_new, row_num, col_num)
    sub('killall Dock', need_res=True)


def main(arg: str = ''):
    if arg == '':
        args = sys.argv[1:]
    else:
        args = arg
    # bugs to fix:
    # TODO - LPD_1: Fix case where more main holding screens are needed - figure out how to get more UUIDs
    # TODO - LPD_2: Fix case where less main holding screens are needed - delete stuff from groups table
    row_num = 4
    col_num = 7
    do_new = False
    row_usage = '-r,--rows    N :\t number of rows to set in springboard\n'
    col_usage = '-c,--columns N :\t number of columns to set in springboard\n'
    do_new_usage = '-n,--donew Y/N :\t use new method for reorganization\n'
    try:
        opts, args = getopt.getopt(args, 'hn:r:c:', ['donew=', 'rows=', 'columns='])
        for opt, arg in opts:
            if opt == '-h':
                print('usage \n' + row_usage + col_usage + do_new_usage)
                sys.exit()
            elif opt in ('-r', '--rows'):
                row_num = int(arg)
            elif opt in ('-c', '--columns'):
                col_num = int(arg)
            elif opt in ('-n', '--donew'):
                do_new = opt.lower()[1].strip() in ['t', 'y']
        set_size_db(row_num, col_num, True, True, True, do_new)
    except getopt.GetoptError as e:
        print('GetOptError with msg:' + e.msg)
        print('usage \n' + row_usage + col_usage + do_new_usage)


if __name__ == '__main__':
    main('-c 7 -r 3 -n true')
